package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.worker.Worker;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static com.vladykin.replicamap.kafka.KReplicaMapManager.State.STOPPED;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_WORKERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerFailureTest.assertStopped;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerFailureTest.hasCause;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerFailureTest.awaitOrFail;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every Kafka client the manager creates must be closed when it stops — both when the application
 * closes it and when it stops itself after an unrecoverable error.
 */
class KReplicaMapResourceReleaseTest {

    /** Client kinds we expect to exist before asserting, so the assertions can not pass vacuously. */
    static final List<String> ALL_KINDS =
        List.of("data-consumer", "ops-consumer", "flush-consumer", "data-producer", "ops-producer", "flush-producer");

    /** Static so that it survives a manager whose constructor throws, and so that the manager's super
     * constructor can record into it before the subclass fields are initialized. Tests run sequentially. */
    static final List<TrackedClient> CLIENTS = new CopyOnWriteArrayList<>();

    static final RuntimeException OPS_POLL_BOOM = new IllegalStateException("boom on ops consumer poll");
    static final RuntimeException FLUSH_SUBSCRIBE_BOOM = new IllegalStateException("boom on flush consumer subscribe");

    /** Fault injection, set by a test before constructing the manager. Static for the same reason as
     * {@link #CLIENTS}: the factories below run inside the super constructor. -1 injects nothing. */
    static int failPollOnOpsConsumer = -1;
    static int failSubscribeOnFlushConsumer = -1;

    static final AtomicInteger opsConsumersCreated = new AtomicInteger();
    static final AtomicInteger flushConsumersCreated = new AtomicInteger();

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    @BeforeEach
    void resetTrackedClients() {
        CLIENTS.clear();

        failPollOnOpsConsumer = -1;
        failSubscribeOnFlushConsumer = -1;

        opsConsumersCreated.set(0);
        flushConsumersCreated.set(0);
    }

    @Test
    void testResourcesReleasedOnClose() throws Exception {
        String data = "res_close";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 4);

        TrackingManager m = new TrackingManager(getConfig(data), null);
        try {
            m.start(60, SECONDS);

            KReplicaMap<String,String> map = m.getMap();
            map.put("k", "v");

            awaitAllKinds();
        }
        finally {
            m.close();
        }

        assertAllClosed();
        assertNull(m.getFailure(), "A cleanly closed manager must not report a failure");
    }

    @Test
    void testResourcesReleasedOnWorkerFailureWithoutClosingManager() throws Exception {
        String data = "res_fail";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 4);

        IllegalStateException boom = new IllegalStateException("boom in update handler");

        // Kills the ops worker that replays the echo of the sentinel key, like KReplicaMapManagerFailureTest.
        TrackingManager m = new TrackingManager(getConfig(data), boom);
        try {
            m.start(60, SECONDS);

            KReplicaMap<String,String> map = m.getMap();

            // Every kind must exist before we kill it, otherwise there is nothing to assert about.
            awaitAllKinds();

            map.asyncPut("__poison__", "v");

            // No close() here: the manager must stop itself and release everything on its own.
            assertStopped(m);
            assertAllClosed();

            // The application can tell this apart from a shutdown it asked for.
            assertTrue(hasCause(m.getFailure(), boom),
                "Expected the injected failure in the cause chain: " + m.getFailure());

            // Closing an already self-stopped manager is a no-op, not an error.
            m.close();
            m.close();
            assertSame(STOPPED, m.getState());
        }
        finally {
            m.close(); // No-op if it already stopped, but must not leak clients if the test failed.
        }
    }

    /**
     * A worker dying before the manager reaches RUNNING makes {@code onWorkersSteady} run the cleanup
     * from a worker thread, where {@code interruptAll} interrupts that very thread and {@code joinAll}
     * cannot wait — leaving the workers unjoined while their clients get closed underneath them. The
     * stop must therefore always run on a thread of its own.
     */
    @Test
    void testStartupFailureCleansUpProperly() throws Exception {
        String data = "res_start_fail";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 4);

        Map<String,Object> cfg = getConfig(data);
        cfg.put(OPS_WORKERS, 2);

        failPollOnOpsConsumer = 0; // Ops worker 0 dies before steady, worker 1 stays healthy.

        TrackingManager m = new TrackingManager(cfg, null);
        try {
            assertThrows(ReplicaMapException.class, () -> m.start(60, SECONDS));
        }
        finally {
            m.close(); // Waits for the stop to finish, whoever started it.
        }

        // close() returned, so the stop is complete: every worker must already be joined and dead.
        for (Worker w : m.opsWorkers)
            assertFalse(w.isAlive(), "Ops worker still running after the stop completed: " + w);

        for (Worker w : m.flushWorkers)
            assertFalse(w.isAlive(), "Flush worker still running after the stop completed: " + w);

        List<String> polledAfterClose = new ArrayList<>();

        for (TrackedClient c : CLIENTS) {
            if (c.polledAfterClose.get() > 0)
                polledAfterClose.add(c.kind);
        }

        assertEquals(List.of(), polledAfterClose, "Clients polled after the stop closed them");

        assertSame(STOPPED, m.getState());
        assertAllClosed();
    }

    /**
     * {@code FlushWorker.newFlushConsumer} subscribes after creating the consumer, and a consumer that
     * fails to subscribe never reaches the LazyList, so nothing else can close it.
     */
    @Test
    void testFlushConsumerClosedWhenSubscribeFails() throws Exception {
        String data = "res_sub_fail";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 4);

        // Only the first one fails: processFlushRequests retries in a tight loop, so failing them all
        // would spin creating consumers.
        failSubscribeOnFlushConsumer = 0;

        TrackingManager m = new TrackingManager(getConfig(data), null);
        try {
            m.start(60, SECONDS);

            // Wait for the retry, which proves the first one failed and was discarded.
            awaitOrFail(SECONDS.toNanos(60), () ->
                flushConsumersCreated.get() >= 2 ? null
                    : "The flush consumer was not retried, created: " + flushConsumersCreated.get());
        }
        finally {
            m.close();
        }

        // The discarded one is unreachable from the manager, so this only holds if newFlushConsumer
        // closed it itself.
        assertAllClosed();
    }

    /**
     * A manager whose construction fails part way must not leak the clients it already created. The
     * workers it did construct are closed by the {@code stop(stopTimeout)} in the constructor's catch,
     * but the consumers of the worker being constructed are reachable only from {@code newOpsWorker}
     * itself, since that worker never makes it into {@code opsWorkers}.
     */
    @Test
    void testResourcesReleasedWhenManagerConstructionFails() throws Exception {
        String data = "res_ctor";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 4);

        IllegalStateException boom = new IllegalStateException("cannot create the ops consumer");

        // The data consumer of the same worker is created first, so it is already live (and tracked)
        // when this one throws. The two use different configs, so failing only one is realistic.
        ReplicaMapException e = assertThrows(ReplicaMapException.class,
            () -> new TrackingManager(getConfig(data), null) {
                @Override
                protected Consumer<Object,OpMessage> newKafkaConsumerOps() {
                    throw boom;
                }
            });

        assertTrue(hasCause(e, boom), "Expected the injected failure in the cause chain: " + e);

        // Not vacuous: the data consumer really was created before the failure.
        assertTrue(CLIENTS.stream().anyMatch(c -> c.kind.equals("data-consumer")),
            "The data consumer was never created, so nothing was at risk of leaking: " + CLIENTS);

        assertAllClosed();
    }

    Map<String,Object> getConfig(String dataTopic) {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));
        cfg.put(DATA_TOPIC, dataTopic);
        cfg.put(OPS_TOPIC, dataTopic + "_ops");
        cfg.put(FLUSH_TOPIC, dataTopic + "_flush");
        return cfg;
    }

    /**
     * Wraps every Kafka client the manager creates into a proxy recording whether it was closed, and
     * optionally kills the ops worker on a sentinel key.
     */
    static class TrackingManager extends KReplicaMapManager {
        final RuntimeException poison;

        TrackingManager(Map<String,Object> cfg, RuntimeException poison) {
            super(cfg);
            this.poison = poison;
        }

        @Override
        protected <K,V> boolean applyReceivedUpdate(String topic, int part, long offset, UUID clientId,
            long opId, byte updateType, K key, V exp, V upd, BiFunction<?,?,?> function,
            java.util.function.Consumer<V> updatedValueBox
        ) {
            if (poison != null && "__poison__".equals(key))
                throw poison;

            return super.applyReceivedUpdate(topic, part, offset, clientId, opId, updateType,
                key, exp, upd, function, updatedValueBox);
        }

        @Override
        protected Consumer<Object,Object> newKafkaConsumerData() {
            return track("data-consumer", Consumer.class, super.newKafkaConsumerData());
        }

        @Override
        protected Consumer<Object,OpMessage> newKafkaConsumerOps() {
            boolean fail = opsConsumersCreated.getAndIncrement() == failPollOnOpsConsumer;

            return track("ops-consumer", Consumer.class, super.newKafkaConsumerOps(),
                fail ? "poll" : null, OPS_POLL_BOOM);
        }

        @Override
        protected Consumer<Object,FlushRequest> newKafkaConsumerFlush() {
            boolean fail = flushConsumersCreated.getAndIncrement() == failSubscribeOnFlushConsumer;

            return track("flush-consumer", Consumer.class, super.newKafkaConsumerFlush(),
                fail ? "subscribe" : null, FLUSH_SUBSCRIBE_BOOM);
        }

        @Override
        protected Producer<Object,Object> newKafkaProducerData(int part) {
            return track("data-producer", Producer.class, super.newKafkaProducerData(part));
        }

        @Override
        protected Producer<Object,OpMessage> newKafkaProducerOps() {
            return track("ops-producer", Producer.class, super.newKafkaProducerOps());
        }

        @Override
        protected Producer<Object,FlushRequest> newKafkaProducerFlush() {
            return track("flush-producer", Producer.class, super.newKafkaProducerFlush());
        }

        <T> T track(String kind, Class<?> iface, T target) {
            return track(kind, iface, target, null, null);
        }

        /**
         * @param kind Client kind, for the assertion messages.
         * @param iface Kafka client interface to proxy ({@code Consumer} or {@code Producer}).
         * @param target Real client to delegate to.
         * @param failMethod Name of the method that must throw instead of delegating, or {@code null}.
         * @param failWith What that method throws.
         * @return Proxy recording {@code close()} and delegating everything else.
         */
        @SuppressWarnings("unchecked")
        <T> T track(String kind, Class<?> iface, T target, String failMethod, RuntimeException failWith) {
            TrackedClient tracked = new TrackedClient(kind);

            T proxy = (T)Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {iface},
                (p, mtd, args) -> {
                    String mtdName = mtd.getName();

                    if ("close".equals(mtdName))
                        tracked.closed.set(true);
                    else if ("poll".equals(mtdName) && tracked.closed.get()) {
                        // A worker is still running and using a client the stop already closed, which
                        // is what happens when the cleanup does not join the workers first.
                        tracked.polledAfterClose.incrementAndGet();
                    }

                    if (mtdName.equals(failMethod))
                        throw failWith;

                    try {
                        return mtd.invoke(target, args);
                    }
                    catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });

            CLIENTS.add(tracked);

            return proxy;
        }
    }

    /**
     * Waits until at least one client of every kind was created: the flush consumer appears only once
     * the ops workers are steady, and the data producers only on the flush partition assignment, so
     * asserting before that would prove nothing.
     */
    static void awaitAllKinds() throws InterruptedException {
        awaitOrFail(SECONDS.toNanos(60), () -> {
            List<String> missing = new ArrayList<>();

            for (String kind : ALL_KINDS) {
                if (CLIENTS.stream().noneMatch(c -> c.kind.equals(kind)))
                    missing.add(kind);
            }

            return missing.isEmpty() ? null : "Clients were never created: " + missing;
        });
    }

    static void assertAllClosed() {
        List<String> open = new ArrayList<>();

        for (TrackedClient c : CLIENTS) {
            if (!c.closed.get())
                open.add(c.kind);
        }

        assertEquals(List.of(), open, "Kafka clients left open");
    }

    static class TrackedClient {
        final String kind;
        final AtomicBoolean closed = new AtomicBoolean();
        final AtomicInteger polledAfterClose = new AtomicInteger();

        TrackedClient(String kind) {
            this.kind = kind;
        }

        @Override
        public String toString() {
            return kind + (closed.get() ? "(closed)" : "(open)");
        }
    }
}
