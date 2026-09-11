package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.compute.ComputeDeserializer;
import com.vladykin.replicamap.kafka.compute.ComputeSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.COMPUTE_DESERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.COMPUTE_SERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_FLUSH_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_OPS_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A map update (synchronous or asynchronous) initiated from within a ReplicaMap update callback runs
 * on the update-processing (ops worker) thread and would dead-lock waiting for its own echo. The
 * {@code beforeNew} guard must reject such an attempt at initiation, while still allowing reads from
 * the callback.
 */
class KReplicaMapReentrancyTest {

    static final String DATA = DEFAULT_DATA_TOPIC;
    static final String OPS = DEFAULT_DATA_TOPIC + DEFAULT_OPS_TOPIC_SUFFIX;
    static final String FLUSH = DEFAULT_DATA_TOPIC + DEFAULT_FLUSH_TOPIC_SUFFIX;

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    Map<String,Object> getDefaultConfig() {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));
        return cfg;
    }

    @Test
    void testUpdateFromListenerIsRejected() throws Exception {
        createTopics(sharedKafkaTestResource, DATA, OPS, FLUSH, 4);

        try (KReplicaMapManager m = new KReplicaMapManager(getDefaultConfig())) {
            m.start(60, SECONDS);
            KReplicaMap<String,String> map = m.getMap();

            AtomicBoolean attempted = new AtomicBoolean();
            AtomicReference<Object> readResult = new AtomicReference<>();
            AtomicReference<Object> syncResult = new AtomicReference<>();
            AtomicReference<Object> asyncResult = new AtomicReference<>();
            CountDownLatch fired = new CountDownLatch(1);

            // The listener runs on the ops-worker thread; it must capture the guard exception itself,
            // because onMapUpdate swallows (logs) any exception a listener throws.
            map.setListener((lmap, myUpdate, key, oldValue, newValue) -> {
                if (!attempted.compareAndSet(false, true))
                    return; // attempt the reentrant updates only once

                // Reads must be allowed from a callback.
                readResult.set(lmap.get(key));

                // A synchronous update must be rejected.
                try {
                    lmap.put("reentrant", "x");
                    syncResult.set("NO EXCEPTION");
                }
                catch (Throwable e) {
                    syncResult.set(e);
                }

                // An asynchronous update must be rejected at initiation too (thrown synchronously).
                try {
                    lmap.asyncPut("reentrant", "x");
                    asyncResult.set("NO EXCEPTION");
                }
                catch (Throwable e) {
                    asyncResult.set(e);
                }

                fired.countDown();
            });

            // Trigger the listener: this update echoes back and fires the listener on the ops thread.
            map.put("trigger", "v");

            assertTrue(fired.await(30, SECONDS), "Listener did not fire");

            assertEquals("v", readResult.get(), "Reads from a callback must be allowed");

            ReplicaMapException syncEx = assertInstanceOf(ReplicaMapException.class, syncResult.get(),
                "Sync update from a callback must be rejected, got: " + syncResult.get());
            assertTrue(syncEx.getMessage().contains("not allowed from a ReplicaMap update"), syncEx.getMessage());

            ReplicaMapException asyncEx = assertInstanceOf(ReplicaMapException.class, asyncResult.get(),
                "Async update from a callback must be rejected, got: " + asyncResult.get());
            assertTrue(asyncEx.getMessage().contains("not allowed from a ReplicaMap update"), asyncEx.getMessage());

            // The rejected updates must not have changed the map.
            assertEquals(1, map.size());
            assertEquals("v", map.get("trigger"));
        }
    }

    /**
     * The other half of the guard's contract: a replicated {@code compute}/{@code merge} function is
     * re-executed on the ops worker thread while the update is applied, so an update issued from
     * inside it dead-locks exactly like one issued from a listener and must be rejected too.
     */
    @Test
    void testUpdateFromComputeFunctionIsRejected() throws Exception {
        String data = "reentrancy_compute";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 4);

        Map<String,Object> cfg = getDefaultConfig();
        cfg.put(DATA_TOPIC, data);
        cfg.put(OPS_TOPIC, data + "_ops");
        cfg.put(FLUSH_TOPIC, data + "_flush");
        cfg.put(COMPUTE_SERIALIZER_CLASS, ReentrantUpdateSerializer.class);
        cfg.put(COMPUTE_DESERIALIZER_CLASS, ReentrantUpdateDeserializer.class);

        try (KReplicaMapManager m = new KReplicaMapManager(cfg)) {
            m.start(60, SECONDS);
            KReplicaMap<String,String> map = m.getMap();

            map.put("k", "v");

            REENTRANT_MAP.set(map);
            REENTRANT_RESULT.set(null);
            try {
                // The function is re-executed on the ops worker while the update is applied, and the
                // guard rejects the nested put there. That failure propagates out of the replicated
                // function, so the compute call fails too and the manager stops itself with an
                // unrecoverable error (the ERROR in the log is expected here) - what this test is
                // about is that the nested update was rejected rather than silently dead-locking.
                assertThrows(ReplicaMapException.class, () -> map.compute("k", new ReentrantUpdate()));

                Object result = REENTRANT_RESULT.get();

                ReplicaMapException e = assertInstanceOf(ReplicaMapException.class, result,
                    "An update from a compute function must be rejected, got: " + result);
                assertTrue(e.getMessage().contains("not allowed from a ReplicaMap update"), e.getMessage());
            }
            finally {
                REENTRANT_MAP.set(null);
            }
        }
    }

    /** The map the {@link ReentrantUpdate} function tries to update from inside itself. */
    static final AtomicReference<KReplicaMap<String,String>> REENTRANT_MAP = new AtomicReference<>();

    /** What the nested update did: the guard exception, or "NO EXCEPTION". */
    static final AtomicReference<Object> REENTRANT_RESULT = new AtomicReference<>();

    /** A replicated remapping function that tries to update the map from within itself. */
    static class ReentrantUpdate implements BiFunction<String,String,String> {
        @Override
        public String apply(String key, String value) {
            KReplicaMap<String,String> map = REENTRANT_MAP.get();

            if (map != null) {
                try {
                    map.put("reentrant", "x");
                    REENTRANT_RESULT.compareAndSet(null, "NO EXCEPTION");
                }
                catch (Throwable e) {
                    REENTRANT_RESULT.compareAndSet(null, e);
                    throw e;
                }
            }

            return value + "!";
        }
    }

    public static class ReentrantUpdateSerializer implements ComputeSerializer {
        @Override
        public boolean canSerialize(BiFunction<?,?,?> function) {
            return function instanceof ReentrantUpdate;
        }

        @Override
        public byte[] serialize(String topic, BiFunction<?,?,?> function) {
            return new byte[0]; // Stateless.
        }
    }

    public static class ReentrantUpdateDeserializer implements ComputeDeserializer {
        @Override
        public BiFunction<?,?,?> deserialize(String topic, byte[] data) {
            return new ReentrantUpdate();
        }
    }
}