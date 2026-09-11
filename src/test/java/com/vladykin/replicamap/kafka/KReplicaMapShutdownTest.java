package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMapException;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static com.vladykin.replicamap.kafka.KReplicaMapManager.State.STOPPED;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_WORKERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerFailureTest.awaitOrFail;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A worker thread must never wait for a shutdown that has to join it.
 *
 * <p>Both {@code onUnrecoverableError} and {@code onWorkersSteady} run on a worker thread and stop the
 * manager, while {@code doStop} interrupts and joins every worker. As long as those two waited for the
 * stop to complete, the only thing that saved them was the stopper's interrupt arriving after they
 * parked. These tests remove that rescue by making {@code doStop} sleep <em>before</em> it interrupts
 * anything: a worker that waits for its own stop stays alive through the sleep, a worker that only
 * triggers it is gone immediately.
 */
class KReplicaMapShutdownTest {
    /** How long the stop is held before it interrupts anyone. */
    static final long STOP_DELAY_MS = 5000;

    /** Assertion window, comfortably inside {@link #STOP_DELAY_MS}. */
    static final long ASSERT_TIMEOUT_NANOS = SECONDS.toNanos(2);

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    Map<String,Object> getConfig(String dataTopic) {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));
        cfg.put(DATA_TOPIC, dataTopic);
        cfg.put(OPS_TOPIC, dataTopic + "_ops");
        cfg.put(FLUSH_TOPIC, dataTopic + "_flush");
        cfg.put(OPS_WORKERS, 1); // A single ops worker, so it is the one that dies below.
        return cfg;
    }

    /** Asserts the only ops worker is no longer running, while the stop is still being held. */
    static void assertOpsWorkerDied(KReplicaMapManager m) throws InterruptedException {
        awaitOrFail(ASSERT_TIMEOUT_NANOS, () -> m.opsWorkers.get(0).isAlive() ?
            "The ops worker is still running: it waited for the stop that has to join it" : null);
    }

    /**
     * The failure route: a worker dies, records the failure and stops the manager from
     * {@code onUnrecoverableError}.
     */
    @Test
    void testDyingWorkerDoesNotWaitForItsOwnStop() throws Exception {
        String data = "shutdown_fail";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 4);

        IllegalStateException boom = new IllegalStateException("boom in update handler");
        CountDownLatch stopEntered = new CountDownLatch(1);

        KReplicaMapManager m = new KReplicaMapManager(getConfig(data)) {
            @Override
            protected void doStop() {
                stopEntered.countDown();
                sleepQuietly(STOP_DELAY_MS);
                super.doStop();
            }

            @Override
            protected <K,V> boolean applyReceivedUpdate(String topic, int part, long offset, UUID clientId,
                long opId, byte updateType, K key, V exp, V upd, BiFunction<?,?,?> function,
                java.util.function.Consumer<V> updatedValueBox
            ) {
                if ("__poison__".equals(key))
                    throw boom;

                return super.applyReceivedUpdate(topic, part, offset, clientId, opId, updateType,
                    key, exp, upd, function, updatedValueBox);
            }
        };

        try {
            m.start(60, SECONDS);

            // The echo of this put is replayed on the ops worker thread, where it kills the worker.
            m.<String,String,KReplicaMap<String,String>>getMap().asyncPut("__poison__", "v");

            assertTrue(stopEntered.await(30, SECONDS), "The manager never started stopping itself");
            assertOpsWorkerDied(m);
        }
        finally {
            m.close();
        }
    }

    /**
     * The startup route: a worker fails before it reaches the steady state, so {@code onWorkersSteady}
     * runs on that worker thread and stops the manager from there.
     */
    @Test
    void testStartupFailureDoesNotWaitForItsOwnStop() throws Exception {
        String data = "shutdown_start";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 4);

        IllegalStateException boom = new IllegalStateException("boom while loading the data topic");
        CountDownLatch stopEntered = new CountDownLatch(1);

        KReplicaMapManager m = new KReplicaMapManager(getConfig(data)) {
            @Override
            protected void doStop() {
                stopEntered.countDown();
                sleepQuietly(STOP_DELAY_MS);
                super.doStop();
            }

            @Override
            protected Consumer<Object,Object> newKafkaConsumerData() {
                // Fails the load, so the worker never becomes steady.
                return failOnPoll(super.newKafkaConsumerData(), boom);
            }
        };

        try {
            assertThrows(ReplicaMapException.class, () -> m.start(60, SECONDS));

            assertTrue(stopEntered.await(30, SECONDS), "The manager never started stopping itself");
            assertOpsWorkerDied(m);
        }
        finally {
            m.close();
        }
    }

    /**
     * {@code stop()} never blocks, so it is safe from a listener, which runs on the ops worker thread
     * the stop has to join. (Map <em>updates</em> from a callback stay rejected, see
     * {@link KReplicaMapReentrancyTest}.)
     */
    @Test
    void testStopFromListenerDoesNotDeadlock() throws Exception {
        String data = "shutdown_lsnr";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 4);

        KReplicaMapManager m = new KReplicaMapManager(getConfig(data));

        try {
            m.start(60, SECONDS);

            KReplicaMap<String,String> map = m.getMap();
            AtomicBoolean stopped = new AtomicBoolean();

            map.setListener((lmap, myUpdate, key, oldValue, newValue) -> {
                if (stopped.compareAndSet(false, true))
                    m.stop(); // On the ops worker thread: must return instead of waiting.
            });

            map.put("k", "v");

            awaitOrFail(SECONDS.toNanos(60),
                () -> m.getState() == STOPPED ? null : "The manager did not stop, state: " + m.getState());
        }
        finally {
            m.close();
        }
    }

    static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Wraps the consumer so that {@code poll} always throws. A {@link Proxy} keeps this to a few lines,
     * the same way {@code KReplicaMapResourceReleaseTest} tracks its clients.
     */
    @SuppressWarnings("unchecked")
    static Consumer<Object,Object> failOnPoll(Consumer<Object,Object> delegate, RuntimeException err) {
        return (Consumer<Object,Object>)Proxy.newProxyInstance(
            Consumer.class.getClassLoader(),
            new Class<?>[] {Consumer.class},
            (proxy, method, args) -> {
                if ("poll".equals(method.getName()))
                    throw err;

                try {
                    return method.invoke(delegate, args);
                }
                catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
    }
}
