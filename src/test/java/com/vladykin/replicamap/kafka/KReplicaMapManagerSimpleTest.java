package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapListener;
import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.kafka.compute.ComputeDeserializer;
import com.vladykin.replicamap.kafka.compute.ComputeSerializer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.COMPUTE_DESERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.COMPUTE_SERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_FLUSH_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_OPS_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_MAX_POLL_TIMEOUT_MS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_PERIOD_OPS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("ConstantConditions")
class KReplicaMapManagerSimpleTest {
    static final int START_TIMEOUT = 60;

    static final String DATA_TOPIC = DEFAULT_DATA_TOPIC;
    static final String OPS_TOPIC = DEFAULT_DATA_TOPIC + DEFAULT_OPS_TOPIC_SUFFIX;
    static final String FLUSH_TOPIC = DEFAULT_DATA_TOPIC + DEFAULT_FLUSH_TOPIC_SUFFIX;

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    static SharedKafkaTestResource kafkaClusterWith3Brokers() {
        return new SharedKafkaTestResource(getBrokerConfig()).withBrokers(3);
    }

    static Properties getBrokerConfig() {
        Properties brokerProperties = new Properties();

        brokerProperties.setProperty("offsets.topic.replication.factor", "3");
        brokerProperties.setProperty("offset.storage.replication.factor", "3");
        brokerProperties.setProperty("transaction.state.log.replication.factor", "3");
        brokerProperties.setProperty("transaction.state.log.min.isr", "2");
        brokerProperties.setProperty("transaction.state.log.num.partitions", "4");
        brokerProperties.setProperty("config.storage.replication.factor", "3");
        brokerProperties.setProperty("status.storage.replication.factor", "3");
        brokerProperties.setProperty("default.replication.factor", "3");

        return brokerProperties;
    }

    Map<String,Object> getDefaultConfig() {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));
        cfg.put(FLUSH_PERIOD_OPS, 4);
        cfg.put(FLUSH_MAX_POLL_TIMEOUT_MS, 1L);

        cfg.put(COMPUTE_SERIALIZER_CLASS, JoinStringsSerializer.class);
        cfg.put(COMPUTE_DESERIALIZER_CLASS, JoinStringsDeserializer.class);

        return cfg;
    }

    static void createTopics(
        SharedKafkaTestResource sharedKafkaTestResource,
        String dataTopic,
        String opsTopic,
        String flushTopic,
        int parts
    ) throws Exception {
        KafkaTestUtils utils = sharedKafkaTestResource.getKafkaTestUtils();
        AdminClient admin = utils.getAdminClient();

        NewTopic ops = new NewTopic(opsTopic, parts, (short)2);
        NewTopic flush = new NewTopic(flushTopic, parts, (short)2);
        NewTopic data = new NewTopic(dataTopic, parts, (short)2);

        Map<String,String> cfg = new HashMap<>();
        cfg.put("min.insync.replicas", "2");
        cfg.put("unclean.leader.election.enable", "false");
        cfg.put("cleanup.policy", "delete");
        cfg.put("retention.bytes", String.valueOf(50L * 1024 * 1024 * 1024));
        cfg.put("file.delete.delay.ms", "3600000");
        cfg.put("retention.ms", "-1");

        ops.configs(new HashMap<>(cfg));
        flush.configs(new HashMap<>(cfg));

        cfg.put("cleanup.policy", "compact");
        cfg.put("min.compaction.lag.ms", "3600000");

        data.configs(new HashMap<>(cfg));

        admin.createTopics(asList(ops, flush, data)).all().get(5, SECONDS);

        assertTrue(utils.consumeAllRecordsFromTopic(dataTopic).isEmpty());
        assertTrue(utils.consumeAllRecordsFromTopic(opsTopic).isEmpty());
        assertTrue(utils.consumeAllRecordsFromTopic(flushTopic).isEmpty());
    }

    @SuppressWarnings({"Convert2MethodRef", "ResultOfMethodCallIgnored"})
    @Test
    void testSimple() throws Exception {
        JoinStringsSerializer.canSerialize = false;
        JoinStrings.executed.set(0);

        createTopics(sharedKafkaTestResource,
            DATA_TOPIC, OPS_TOPIC, FLUSH_TOPIC, 5);

        KReplicaMapManager m = new KReplicaMapManager(getDefaultConfig());

        assertEquals(DATA_TOPIC, m.getDataTopic());
        assertEquals(OPS_TOPIC, m.getOpsTopic());
        assertEquals(FLUSH_TOPIC, m.getFlushTopic());
        assertEquals(5, m.getTotalPartitions());

        assertSame(KReplicaMapManager.State.NEW, m.getState());
        CompletableFuture<ReplicaMapManager> startFut = m.start();
        assertSame(KReplicaMapManager.State.STARTING, m.getState());
        assertSame(m, startFut.get(START_TIMEOUT, SECONDS));
        assertSame(KReplicaMapManager.State.RUNNING, m.getState());

        KReplicaMap<String,String> map = m.getMap();

        assertSame(m, map.getManager());

        Object mapId = map.id();

        assertSame(map, m.getMap());
        assertSame(map, m.getMap(mapId));

        assertTrue(map.isEmpty());

        map.put("a", "A");
        map.put("b", "B");

        assertEquals(2, map.size());
        assertEquals("A", map.get("a"));
        assertEquals("B", map.get("b"));

        System.out.println(m);
        System.out.println(map);

        m.close();
        assertSame(KReplicaMapManager.State.STOPPED, m.getState());
        m.stop();
        assertSame(KReplicaMapManager.State.STOPPED, m.getState());

        KReplicaMap<String,String> finalMap = map;
        assertThrows(ReplicaMapException.class, () -> finalMap.isEmpty());
        assertThrows(ReplicaMapException.class, () -> finalMap.put("c", "C"));

        m = new KReplicaMapManager(getDefaultConfig());
        assertSame(m, m.start().get(START_TIMEOUT, SECONDS));

        assertNotSame(map, m.getMap());
        map = m.getMap();
        assertEquals(mapId, map.id());

        assertEquals(2, map.size());
        assertEquals("A", map.get("a"));
        assertEquals("B", map.get("b"));

        map.put("c", "C");
        map.put("d", "D");

        assertEquals(4, map.size());
        assertEquals("A", map.get("a"));
        assertEquals("B", map.get("b"));
        assertEquals("C", map.get("c"));
        assertEquals("D", map.get("d"));

        m.close();
        m = new KReplicaMapManager(getDefaultConfig());
        assertSame(m, m.start().get(START_TIMEOUT, SECONDS));
        map = m.getMap();

        assertEquals(4, map.size());
        assertEquals("A", map.get("a"));
        assertEquals("B", map.get("b"));
        assertEquals("C", map.get("c"));
        assertEquals("D", map.get("d"));

        map.remove("a", "A");
        map.remove("b", "D");
        map.remove("c");
        map.remove("d", "B");

        m.close();
        m = new KReplicaMapManager(getDefaultConfig());
        assertSame(m, m.start().get(START_TIMEOUT, SECONDS));
        map = m.getMap();

        assertEquals(2, map.size());
        assertEquals("B", map.get("b"));
        assertEquals("D", map.get("d"));

        map.clear();
        assertTrue(map.isEmpty());

        m.close();
        m = new KReplicaMapManager(getDefaultConfig());
        KReplicaMapManager w = new KReplicaMapManager(getDefaultConfig());

        assertSame(m, m.start().get(START_TIMEOUT, SECONDS));
        assertSame(w, w.start().get(START_TIMEOUT, SECONDS));

        KReplicaMap<String,String> mMap = m.getMap();
        KReplicaMap<String,String> wMap = w.getMap();

        assertTrue(mMap.isEmpty());
        assertNotSame(mMap, wMap);
        assertNotSame(mMap.unwrap(), wMap.unwrap());
        assertEquals(mMap.unwrap(), wMap.unwrap());

        listen(mMap, wMap,
            "a", null, "A",
            "b", null, "B");
        mMap.asyncPut("a", "A");
        wMap.asyncPut("b", "B");
        awaitEqualMaps(mMap, wMap,
            "a", "A", "b", "B");

        listen(mMap, wMap,
            "a", "A", "Z",
            "b", "B", null);
        mMap.asyncRemove("b");
        wMap.asyncReplace("a", "Z");
        awaitEqualMaps(mMap, wMap,
            "a", "Z");

        m.close();
        m = new KReplicaMapManager(getDefaultConfig());
        assertSame(m, m.start().get(START_TIMEOUT, SECONDS));
        mMap = m.getMap();

        awaitEqualMaps(mMap, wMap,
            "a", "Z");

        mMap.asyncPut("z", "Z");

        w.close();
        w = new KReplicaMapManager(getDefaultConfig());
        assertSame(w, w.start().get(START_TIMEOUT, SECONDS));
        wMap = w.getMap();

        awaitEqualMaps(mMap, wMap,
            "a", "Z", "z", "Z");

        listen(mMap, wMap,
            "a", "Z", "Zq");
        JoinStringsSerializer.canSerialize = false;
        mMap.compute("a", new JoinStrings("q"));
        awaitEqualMaps(mMap, wMap,
            "a", "Zq", "z", "Z");
        assertEquals(1, JoinStrings.executed.getAndSet(0));

        listen(mMap, wMap,
            "z", "Z", "Zw");
        JoinStringsSerializer.canSerialize = true;
        mMap.compute("z", new JoinStrings("w"));
        awaitEqualMaps(mMap, wMap,
            "a", "Zq", "z", "Zw");
        assertEquals(2, JoinStrings.executed.getAndSet(0));

        listen(mMap, wMap,
            "n", null, "N");
        JoinStringsSerializer.canSerialize = true;
        mMap.computeIfAbsent("n", (k) -> {
            JoinStrings.executed.incrementAndGet();
            return "N";
        });
        awaitEqualMaps(mMap, wMap,
            "a", "Zq", "z", "Zw", "n", "N");
        assertEquals(1, JoinStrings.executed.getAndSet(0));

        listen(mMap, wMap);
        JoinStringsSerializer.canSerialize = true;
        mMap.computeIfAbsent("n", (k) -> {
            JoinStrings.executed.incrementAndGet();
            return "www";
        });
        awaitEqualMaps(mMap, wMap,
            "a", "Zq", "z", "Zw", "n", "N");
        assertEquals(0, JoinStrings.executed.getAndSet(0));

        listen(mMap, wMap,
            "a", "Zq", "Zqe");
        JoinStringsSerializer.canSerialize = true;
        mMap.computeIfPresent("a", new JoinStrings("e"));
        awaitEqualMaps(mMap, wMap,
            "a", "Zqe", "z", "Zw", "n", "N");
        assertEquals(2, JoinStrings.executed.getAndSet(0));

        listen(mMap, wMap,
            "n", "N", "Nr");
        JoinStringsSerializer.canSerialize = false;
        mMap.computeIfPresent("n", new JoinStrings("r"));
        awaitEqualMaps(mMap, wMap,
            "a", "Zqe", "z", "Zw", "n", "Nr");
        assertEquals(1, JoinStrings.executed.getAndSet(0));

        listen(mMap, wMap,
            "b", null, "H");
        JoinStringsSerializer.canSerialize = true;
        mMap.merge("b", "H", new JoinStrings("p"));
        awaitEqualMaps(mMap, wMap,
            "a", "Zqe", "z", "Zw", "n", "Nr", "b", "H");
        assertEquals(0, JoinStrings.executed.getAndSet(0));

        listen(mMap, wMap,
            "c", null, "U");
        JoinStringsSerializer.canSerialize = false;
        mMap.merge("c", "U", new JoinStrings("p"));
        awaitEqualMaps(mMap, wMap,
            "a", "Zqe", "z", "Zw", "n", "Nr", "b", "H", "c", "U");
        assertEquals(0, JoinStrings.executed.getAndSet(0));

        m.close();
        m = new KReplicaMapManager(getDefaultConfig());
        assertSame(m, m.start().get(START_TIMEOUT, SECONDS));
        mMap = m.getMap();
        JoinStrings.executed.set(0);

        awaitEqualMaps(mMap, wMap,
            "a", "Zqe", "z", "Zw", "n", "Nr", "b", "H", "c", "U");

        listen(mMap, wMap,
            "b", "H", "Xp");
        JoinStringsSerializer.canSerialize = true;
        mMap.merge("b", "X", new JoinStrings("p"));
        awaitEqualMaps(mMap, wMap,
            "a", "Zqe", "z", "Zw", "n", "Nr", "b", "Xp", "c", "U");
        assertEquals(2, JoinStrings.executed.getAndSet(0));

        listen(mMap, wMap,
            "c", "U", "Up");
        JoinStringsSerializer.canSerialize = false;
        mMap.merge("c", "U", new JoinStrings("p"));
        awaitEqualMaps(mMap, wMap,
            "a", "Zqe", "z", "Zw", "n", "Nr", "b", "Xp", "c", "Up");
        assertEquals(1, JoinStrings.executed.getAndSet(0));

        w.close();
        w = new KReplicaMapManager(getDefaultConfig());
        assertSame(w, w.start().get(START_TIMEOUT, SECONDS));
        wMap = w.getMap();
        JoinStrings.executed.set(0);

        awaitEqualMaps(mMap, wMap,
            "a", "Zqe", "z", "Zw", "n", "Nr", "b", "Xp", "c", "Up");

        listen(mMap, wMap,
            "a", "Zqe", "A",
            "z", "Zw", "X",
            "c", "Up", "F");
        Map<String, String> x = new HashMap<>();
        x.put("a", "A");
        x.put("z", "X");
        x.put("c", "F");
        wMap.putAll(x);
        awaitEqualMaps(mMap, wMap,
            "a", "A", "z", "X", "n", "Nr", "b", "Xp", "c", "F");

        listen(mMap, wMap,
            "a", "A", "Ao",
            "z", "X", "Xo",
            "n", "Nr", "Nro",
            "b", "Xp", "Xpo",
            "c", "F", "Fo");
        JoinStringsSerializer.canSerialize = true;
        mMap.replaceAll(new JoinStrings("o"));
        awaitEqualMaps(mMap, wMap,
            "a", "Ao", "z", "Xo", "n", "Nro", "b", "Xpo", "c", "Fo");
        assertEquals(10, JoinStrings.executed.getAndSet(0));

        listen(mMap, wMap,
            "a", "Ao", "Aok",
            "z", "Xo", "Xok",
            "n", "Nro", "Nrok",
            "b", "Xpo", "Xpok",
            "c", "Fo", "Fok");
        JoinStringsSerializer.canSerialize = false;
        wMap.replaceAll(new JoinStrings("k"));
        awaitEqualMaps(mMap, wMap,
            "a", "Aok", "z", "Xok", "n", "Nrok", "b", "Xpok", "c", "Fok");
        assertEquals(5, JoinStrings.executed.getAndSet(0));

        m.close();
        m = new KReplicaMapManager(getDefaultConfig());
        assertSame(m, m.start().get(START_TIMEOUT, SECONDS));
        mMap = m.getMap();

        awaitEqualMaps(mMap, wMap,
            "a", "Aok", "z", "Xok", "n", "Nrok", "b", "Xpok", "c", "Fok");

        listen(mMap, wMap,
            "a", "Aok", null,
            "z", "Xok", null,
            "n", "Nrok", null,
            "b", "Xpok", null,
            "c", "Fok", null
        );
        mMap.clear();
        awaitEqualMaps(mMap, wMap);

        m.close();
        w.close();
    }

    @SuppressWarnings("BusyWait")
    static void awaitEqualMaps(ReplicaMap<?,?> x, ReplicaMap<?,?> y, String... keyVals) throws Exception {
        if (x == null)
            assertNull(y);
        else {
            long start = System.nanoTime();
            outer: while (NANOSECONDS.toMillis(System.nanoTime() - start) < 1000) {
                if (x.size() != y.size() || x.size() != keyVals.length / 2) {
                    Thread.sleep(1);
                    continue;
                }

                for (int i = 0; i < keyVals.length; i += 2) {
                    String k = keyVals[i];
                    String v = keyVals[i + 1];

                    if (!Objects.equals(v, x.get(k)) || !Objects.equals(v, y.get(k))) {
                        Thread.sleep(1);
                        continue outer;
                    }
                }

                ReplicaMapListener<?,?> lsnr = x.getListener();
                if (lsnr != null)
                    ((TestListener)lsnr).assertAllFired();

                lsnr = y.getListener();
                if (lsnr != null)
                    ((TestListener)lsnr).assertAllFired();

                return;
            }

            fail("Maps are not equal: " + x.unwrap() + " vs " + y.unwrap());
        }
    }

    static class JoinStrings implements BiFunction<String,String,String> {
        static final AtomicInteger executed = new AtomicInteger();

        final String x;

        public JoinStrings(String x) {
            this.x = x;
        }

        @Override
        public String apply(String k, String v) {
            executed.incrementAndGet();
            return v + x;
        }
    }

    public static class JoinStringsSerializer implements ComputeSerializer {
        static volatile boolean canSerialize = true;

        @Override
        public boolean canSerialize(BiFunction<?,?,?> function) {
            return canSerialize && function instanceof JoinStrings;
        }

        @Override
        public byte[] serialize(String topic, BiFunction<?,?,?> function) {
            return ((JoinStrings)function).x.getBytes(UTF_8);
        }
    }

    public static class JoinStringsDeserializer implements ComputeDeserializer {
        @Override
        public BiFunction<?,?,?> deserialize(String topic, byte[] data) {
            return new JoinStrings(new String(data, UTF_8));
        }
    }

    static void listen(ReplicaMap<String,String> map1, ReplicaMap<String,String> map2, String... expected) {
        new TestListener(map1, expected);
        new TestListener(map2, expected);
    }

    static class TestListener implements ReplicaMapListener<String,String> {
        final Map<String,String> expectedUpdates = new ConcurrentHashMap<>();
        final ReplicaMap<String,String> map;

        TestListener(ReplicaMap<String,String> map, String... expected) {
            for (int i = 0; i < expected.length; i += 3)
                expectedUpdates.put(expected[i], expected[i + 1] + "->" + expected[i + 2]);

            this.map = map;
            map.setListener(this);
//            System.out.println();
        }

        @SuppressWarnings("BusyWait")
        void assertAllFired() throws InterruptedException {
            long start = System.nanoTime();
            // Need to wait here because this method is called when the maps are equal,
            // but listener is invoked after map update.
            while (!expectedUpdates.isEmpty()) {
                Thread.sleep(1);

                if (System.nanoTime() - start > SECONDS.toNanos(10))
                    fail("Timeout");
            }
            map.setListener(null);
        }

        @Override
        public void onMapUpdate(
            ReplicaMap<String,String> map,
            boolean myUpdate,
            String key,
            String oldValue,
            String newValue
        ) {
//            System.out.println(key + " = " + oldValue + " -> " + newValue + "   :   " + expectedUpdates + "   :   " + map.unwrap());
            assertTrue(expectedUpdates.remove(key, oldValue + "->" + newValue));
        }
    }
}