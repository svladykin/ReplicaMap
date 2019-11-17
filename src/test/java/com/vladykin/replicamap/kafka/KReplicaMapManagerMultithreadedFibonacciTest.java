package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.holder.MapsHolderMulti;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.vladykin.replicamap.base.ReplicaMapBaseMultithreadedTest.executeThreads;
import static com.vladykin.replicamap.kafka.KReplicaMapManager.State.RUNNING;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_PERIOD_OPS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_WORKERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.MAPS_HOLDER;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_WORKERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.VALUE_DESERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.VALUE_SERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class KReplicaMapManagerMultithreadedFibonacciTest {

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    Map<String,Object> getDefaultConfig() {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));

        cfg.put(FLUSH_PERIOD_OPS, 30);

        cfg.put(DATA_TOPIC, "data");
        cfg.put(OPS_TOPIC, "ops");
        cfg.put(FLUSH_TOPIC, "flush");

        cfg.put(OPS_WORKERS, 4);
        cfg.put(FLUSH_WORKERS, 2);

        cfg.put(MAPS_HOLDER, TestMapsHolder.class);

        cfg.put(VALUE_SERIALIZER_CLASS, BigIntSerializer.class);
        cfg.put(VALUE_DESERIALIZER_CLASS, BigIntDeserializer.class);

        return cfg;
    }

    @Test
    void testFibonacciWithRestart() throws Exception {
        int threadsCnt = 9;
        int managersCnt = 5;
        int mapsCnt = 3;

        createTopics(sharedKafkaTestResource,
            "data", "ops", "flush", mapsCnt);

        LazyList<KReplicaMapManager> managers = new LazyList<>(managersCnt);
        IntFunction<KReplicaMapManager> managersFactory =  (x) -> {
            KReplicaMapManager manager = new KReplicaMapManager(getDefaultConfig());
            manager.start(300, SECONDS);
            System.out.println("started: " + x);
            return manager;
        };

        ExecutorService exec = Executors.newFixedThreadPool(threadsCnt);

        try {
            for (int i = 0; i < 10; i++) {
                CyclicBarrier start = new CyclicBarrier(threadsCnt);

                CompletableFuture<?> fut = Utils.allOf(executeThreads(threadsCnt, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    start.await();

                    for (int j = 0; j < 100; j++) {
                        int mgrId = rnd.nextInt(managersCnt);
                        int mapId = rnd.nextInt(mapsCnt);

                        KReplicaMapManager m = managers.get(mgrId, managersFactory);

                        try {
                            if (rnd.nextInt(300) == 0) {
                                managers.reset(mgrId, m);
                                continue;
                            }

                            KReplicaMap<String,BigInteger> map = m.getMap(mapId);
                            doFibonnacciRun(mapId, map);
                        }
                        catch (ReplicaMapException e) {
                            // may happen if another thread concurrently closed our manager
                        }

//                        if (j % 10 == 0)
//                            System.out.println(j);
                    }

                    return null;
                }));

                fut.get(180, SECONDS);

                System.out.println("checking " + i);

                KReplicaMapManager lastAliveManager = null;

                for (int mgrId = 0; mgrId < managersCnt; mgrId++) {
                    KReplicaMapManager m = managers.get(mgrId, null);

                    if (m != null && m.getState() == RUNNING) {
                        lastAliveManager = m;

                        for (int mapId = 0; mapId < mapsCnt; mapId++) {
                            KReplicaMap<String,BigInteger> map = m.getMap(mapId);

                            if (!map.isEmpty())
                                assertFibonacci(mapId, map, true);
                        }
                    }
                }

                if (lastAliveManager != null) {
                    for (int mapId = 0; mapId < mapsCnt; mapId++)
                        lastAliveManager.getMap(mapId).clear();
                }

                for (int mgrId = 0; mgrId < managersCnt; mgrId++) {
                    KReplicaMapManager m = managers.get(mgrId, null);

                    if (m != null)
                        managers.reset(mgrId, m);
                }

                System.out.println("iteration " + i + " OK");
            }
        }
        finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(10, SECONDS));
            Utils.close(managers);
        }
    }

//    @Test
    void testFibonnacciConcurrentMap() throws Exception {
        int threadsCnt = 37;
        int mapsCnt = 5;

        ExecutorService exec = Executors.newFixedThreadPool(threadsCnt);

        try {
            AtomicLong mapUpdates = new AtomicLong();
            Map<String,BigInteger> map = new ConcurrentSkipListMap<>();

            for (int i = 0; i < 50; i++) {
                long startUpdates = mapUpdates.get();

                CyclicBarrier start = new CyclicBarrier(threadsCnt);

                CompletableFuture<?> fut = Utils.allOf(executeThreads(threadsCnt, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    start.await();

                    for (int j = 0; j < 50_000; j++) {
                        if (doFibonnacciRun(rnd.nextInt(mapsCnt), map))
                            mapUpdates.incrementAndGet();
                    }

                    return null;
                }));

                fut.get(15, SECONDS);

                for (int mapId = 0; mapId < mapsCnt; mapId++)
                    assertFibonacci(mapId, map, false);

                assertTrue(mapUpdates.get() > startUpdates);

                map.clear();
                System.out.println("iteration " + i + " OK");
//                System.out.println(map);
            }
        }
        finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(3, SECONDS));
        }
    }

    boolean doFibonnacciRun(long mapId, Map<String,BigInteger> map) throws InterruptedException {
//        if (map.size() < 3)
            initFibonacci(mapId, map);

        BigInteger a, b, c;
        for(int i = 0;;i++) {
            a = get(mapId, map, "a");
            b = get(mapId, map, "b");
            c = get(mapId, map, "c");

            if (isFibonacci(a,b,c))
                break;

            if (i % 3000 == 0)
                System.out.println("mapId: " + mapId + "\na = " + a + "\nb = " + b + "\nc = " + c + "\n");

            Thread.sleep(1);
        }

        return nextFibonacci(mapId, map, a, b, c);
    }

    static void assertFibonacci(long mapId, Map<String,BigInteger> map, boolean await) throws InterruptedException {
        for (;;) {
            BigInteger a = get(mapId, map, "a");
            BigInteger b = get(mapId, map, "b");
            BigInteger c = get(mapId, map, "c");

            if (isFibonacci(a, b, c))
                break;

            if (await)
                Thread.sleep(1);
            else
                fail();
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    static void initFibonacci(long mapId, Map<String,BigInteger> map) {
        map.putIfAbsent(key(mapId, "a"), BigInteger.valueOf(1L));
        map.putIfAbsent(key(mapId, "b"), BigInteger.valueOf(2L));
        map.putIfAbsent(key(mapId, "c"), BigInteger.valueOf(3L));
    }

    static String key(long mapId, String key) {
        return mapId + " " + key;
    }

    static <X> X get(long mapId, Map<String,X> map, String key) {
        return map.get(key(mapId, key));
    }

    static boolean replace(long mapId, Map<String,BigInteger> map, String key, BigInteger oldVal, BigInteger newVal) {
        return map.replace(key(mapId, key), oldVal, newVal);
    }

    static boolean nextFibonacci(long mapId, Map<String,BigInteger> map, BigInteger a, BigInteger b, BigInteger c) {
        // replace the minimum value with the sum of the other two
        if (a.equals(b.add(c))) {
            if (b.compareTo(c) > 0)
                return replace(mapId, map, "c", c, a.add(b));
            else
                return replace(mapId, map, "b", b, a.add(c));
        }
        else if (b.equals(a.add(c))) {
            if (a.compareTo(c) > 0)
                return replace(mapId, map, "c", c, a.add(b));
            else
                return replace(mapId, map, "a", a, c.add(b));
        }
        else if (c.equals(a.add(b))) {
            if (a.compareTo(b) > 0)
                return replace(mapId, map, "b", b, a.add(c));
            else
                return replace(mapId, map, "a", a, c.add(b));
        }
        else
            fail("Not a Fibonacci sequence " + a + " " + b + " " + c);

        return false;
    }

    static boolean isFibonacci(BigInteger a, BigInteger b, BigInteger c) {
        if (a == null || b == null || c == null)
            return false;

        return a.add(b).equals(c) || a.add(c).equals(b) || b.add(c).equals(a);
    }

    public static class TestMapsHolder extends MapsHolderMulti {
        @Override
        public <K> Object getMapId(K key) {
            String k = (String)key;
            int space = k.indexOf(' ');
            String mapId = k.substring(0, space);
            return Integer.parseInt(mapId);
        }

        @SuppressWarnings("SortedCollectionWithNonComparableKeys")
        @Override
        protected <K, V> Map<K,V> createInnerMap(Object mapId) {
            return (((Number)mapId).intValue() & 1) == 0 ?
                new ConcurrentHashMap<>() :
                new ConcurrentSkipListMap<>();
        }
    }

    public static class BigIntSerializer implements Serializer<BigInteger> {
        @Override
        public byte[] serialize(String topic, BigInteger data) {
            return data == null ? null : data.toByteArray();
        }
    }

    public static class BigIntDeserializer implements Deserializer<BigInteger> {
        @Override
        public BigInteger deserialize(String topic, byte[] data) {
            return data == null ? null : new BigInteger(data);
        }
    }
}