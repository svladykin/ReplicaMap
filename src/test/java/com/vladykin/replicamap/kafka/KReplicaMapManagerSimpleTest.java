package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_FLUSH_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_OPS_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_PERIOD_OPS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_MAX_POLL_TIMEOUT_MS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_MIN_OPS;
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
        cfg.put(FLUSH_MIN_OPS, 1);
        cfg.put(FLUSH_MAX_POLL_TIMEOUT_MS, 1L);
        return cfg;
    }

    static void createTopics(AdminClient admin, String dataTopic, String opsTopic, String flushTopic, int parts) throws Exception {
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
    }

    @SuppressWarnings({"Convert2MethodRef", "ResultOfMethodCallIgnored"})
    @Test
    void testSimple() throws Exception {
        createTopics(sharedKafkaTestResource.getKafkaTestUtils().getAdminClient(),
            DATA_TOPIC, OPS_TOPIC, FLUSH_TOPIC, 5);

        KReplicaMapManager m = new KReplicaMapManager(getDefaultConfig());

        assertSame(KReplicaMapManager.State.NEW, m.getState());
        CompletableFuture<ReplicaMapManager> startFut = m.start();
        assertSame(KReplicaMapManager.State.STARTING, m.getState());
        assertSame(m, startFut.get(START_TIMEOUT, SECONDS));
        assertSame(KReplicaMapManager.State.RUNNING, m.getState());

        KReplicaMap<String,String> map = m.getMap();

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

        mMap.asyncPut("a", "A");
        wMap.asyncPut("b", "B");

        awaitEqualMaps(mMap, wMap,
            "a", "A", "b", "B");

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

        m.close();
        w.close();
    }

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

                return;
            }

            fail("Maps are not equal: " + x.unwrap() + " vs " + y.unwrap());
        }
    }
}