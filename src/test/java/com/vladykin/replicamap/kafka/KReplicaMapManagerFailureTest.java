package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_FLUSH_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_OPS_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManager.State.STOPPED;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * When a worker dies (an unrecoverable error is recorded), the in-flight operations must complete
 * exceptionally instead of hanging forever waiting for an echo that will never be processed, and all
 * subsequent map operations must fail fast.
 */
class KReplicaMapManagerFailureTest {

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

    Map<String,Object> getConfig(String dataTopic) {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));
        cfg.put(DATA_TOPIC, dataTopic);
        cfg.put(OPS_TOPIC, dataTopic + "_ops");
        cfg.put(FLUSH_TOPIC, dataTopic + "_flush");
        return cfg;
    }

    @Test
    void testPendingOpsFailOnUnrecoverableError() throws Exception {
        createTopics(sharedKafkaTestResource, DATA, OPS, FLUSH, 4);

        try (KReplicaMapManager m = new KReplicaMapManager(getDefaultConfig())) {
            m.start(60, SECONDS);
            KReplicaMap<String,String> map = m.getMap();

            CountDownLatch echoArrived = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);

            // The listener runs on the ops-worker thread: blocking it stalls echo processing for the
            // partition, keeping the next update on the same key pending.
            map.setListener((lmap, myUpdate, key, oldValue, newValue) -> {
                echoArrived.countDown();
                try {
                    release.await(60, SECONDS);
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });

            try {
                map.asyncPut("k", "v1"); // Blocks the ops worker in the listener on its echo.
                assertTrue(echoArrived.await(30, SECONDS), "Listener did not fire");

                // Same key -> same partition -> same (stuck) ops worker: this echo cannot be processed.
                CompletableFuture<String> pending = map.asyncPut("k", "v2");
                assertFalse(pending.isDone());

                IllegalStateException boom = new IllegalStateException("boom");
                m.onUnrecoverableError(boom);

                // The pending op must complete exceptionally instead of hanging.
                ExecutionException e = assertThrows(ExecutionException.class, () -> pending.get(10, SECONDS));
                ReplicaMapException cause = assertInstanceOf(ReplicaMapException.class, e.getCause());
                assertSame(boom, cause.getCause());

                // Subsequent operations fail fast with the recorded failure as the cause.
                ReplicaMapException putEx = assertThrows(ReplicaMapException.class, () -> map.put("other", "x"));
                assertTrue(hasCause(putEx, boom), "Expected the recorded failure in the cause chain: " + putEx);
            }
            finally {
                release.countDown(); // Unblock the ops worker so close() does not wait for the listener.
            }
        }
    }

    @Test
    void testRealWorkerDeathFailsOps() throws Exception {
        // Own topics so this test's createTopics does not collide with the other test in this class.
        String data = "worker_death";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 4);

        IllegalStateException boom = new IllegalStateException("boom in update handler");

        // A KReplicaMapManager whose ops worker throws while applying the echo of a sentinel key. This
        // drives the *real* death path (applyReceivedUpdate -> processOps -> doRun's catch(Throwable) ->
        // onUnrecoverableError), unlike testPendingOpsFailOnUnrecoverableError which calls it directly.
        try (KReplicaMapManager m = new KReplicaMapManager(getConfig(data)) {
            @Override
            protected <K,V> boolean applyReceivedUpdate(String topic, int part, long offset, UUID clientId,
                long opId, byte updateType, K key, V exp, V upd, BiFunction<?,?,?> function,
                Consumer<V> updatedValueBox
            ) {
                if ("__poison__".equals(key))
                    throw boom;

                return super.applyReceivedUpdate(topic, part, offset, clientId, opId, updateType,
                    key, exp, upd, function, updatedValueBox);
            }
        }) {
            m.start(60, SECONDS);
            KReplicaMap<String,String> map = m.getMap();

            // The echo of this put is replayed on the ops-worker thread, where applyReceivedUpdate
            // throws and kills the worker; the in-flight op must then complete exceptionally.
            CompletableFuture<String> pending = map.asyncPut("__poison__", "v");

            ExecutionException e = assertThrows(ExecutionException.class, () -> pending.get(30, SECONDS));
            assertTrue(hasCause(e, boom), "Expected the injected failure in the cause chain: " + e);

            // Subsequent operations fail fast with the recorded failure as the cause.
            ReplicaMapException putEx = assertThrows(ReplicaMapException.class, () -> map.put("other", "x"));
            assertTrue(hasCause(putEx, boom), "Expected the recorded failure in the cause chain: " + putEx);

            // The manager must stop itself: all the other workers exit too (a flush worker that kept
            // running would hold its flush partitions in the consumer group while its flush queues no
            // longer advance, so nobody could flush them), and every Kafka client is released.
            assertAllWorkersStopped(m);
            assertStopped(m);

            // The application can tell this apart from a shutdown it asked for.
            assertTrue(hasCause(m.getFailure(), boom),
                "Expected the injected failure in the cause chain: " + m.getFailure());
        }
    }

    static void assertAllWorkersStopped(KReplicaMapManager m) throws InterruptedException {
        awaitOrFail(SECONDS.toNanos(30), () -> {
            List<Worker> alive = new ArrayList<>();

            for (Worker w : m.opsWorkers) {
                if (w.isAlive())
                    alive.add(w);
            }

            for (Worker w : m.flushWorkers) {
                if (w.isAlive())
                    alive.add(w);
            }

            return alive.isEmpty() ? null : "Workers are still running after the manager failed: " + alive;
        });
    }

    /** Asserts the manager stopped itself, without anyone calling {@code close()}. */
    static void assertStopped(KReplicaMapManager m) throws InterruptedException {
        awaitOrFail(SECONDS.toNanos(30),
            () -> m.getState() == STOPPED ? null : "Manager did not stop itself, state: " + m.getState());
    }

    /**
     * Waits until the check returns {@code null}, failing with the message it returns otherwise.
     *
     * @param timeoutNanos Timeout.
     * @param check Returns {@code null} when satisfied, else the failure message to report.
     */
    static void awaitOrFail(long timeoutNanos, Supplier<String> check) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutNanos;

        for (;;) {
            String failureMsg = check.get();

            if (failureMsg == null)
                return;

            if (System.nanoTime() > deadline)
                fail(failureMsg);

            Thread.sleep(50);
        }
    }

    static boolean hasCause(Throwable e, Throwable cause) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t == cause)
                return true;
        }

        return false;
    }
}
