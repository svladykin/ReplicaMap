package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.vladykin.replicamap.base.ReplicaMapBaseMultithreadedTest.executeThreads;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_MIN_OPS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_PERIOD_OPS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_WORKERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.KEY_DESERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.KEY_SERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.MAPS_HOLDER;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_WORKERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.VALUE_DESERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.VALUE_SERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerMultithreadedIncrementSimpleTest.checkFlushedData;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KReplicaMapManagerMultithreadedIncrementRestartTest {

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
        .withBrokers(3);

    Map<String,Object> getDefaultConfig() {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));

        cfg.put(FLUSH_PERIOD_OPS, 30);
        cfg.put(FLUSH_MIN_OPS, 10);

        cfg.put(DATA_TOPIC, "data");
        cfg.put(OPS_TOPIC, "ops");
        cfg.put(FLUSH_TOPIC, "flush");

        cfg.put(OPS_WORKERS, 4);
        cfg.put(FLUSH_WORKERS, 2);

        cfg.put(MAPS_HOLDER, KReplicaMapManagerMultithreadedIncrementSimpleTest.SkipListMapHolder.class);

        cfg.put(KEY_SERIALIZER_CLASS, IntegerSerializer.class);
        cfg.put(KEY_DESERIALIZER_CLASS, IntegerDeserializer.class);

        cfg.put(VALUE_SERIALIZER_CLASS, LongSerializer.class);
        cfg.put(VALUE_DESERIALIZER_CLASS, LongDeserializer.class);

        return cfg;
    }

    @Test
    void testMultithreadedIncrementWithRestart() throws Exception {
        int threadsCnt = 12;
        int managersCnt = 5;
        int keys = 3;

        createTopics(sharedKafkaTestResource.getKafkaTestUtils().getAdminClient(),
            "data", "ops", "flush", keys);

        LazyList<KReplicaMapManager> managers = new LazyList<>(managersCnt);
        IntFunction<KReplicaMapManager> managersFactory =  (x) -> {
            KReplicaMapManager manager = new KReplicaMapManager(getDefaultConfig());
            manager.start(120, SECONDS);
            System.out.println("started: " + x);
            return manager;
        };

        AtomicLong[] cnts = new AtomicLong[keys];
        for (int i = 0; i < keys; i++)
            cnts[i] = new AtomicLong();

        Map<TopicPartition, Long> offsets = new HashMap<>();
        try (Consumer<Object,Object> dataConsumer =
                 managers.get(0, managersFactory).newKafkaConsumerData()) {
            checkFlushedData("data", dataConsumer, offsets, true);
        }

        ExecutorService exec = Executors.newFixedThreadPool(threadsCnt);

        try {
            for (int i = 0; i < 10; i++) {
                CyclicBarrier start = new CyclicBarrier(threadsCnt);

                CompletableFuture<?> fut = Utils.allOf(executeThreads(threadsCnt, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    start.await();

                    for (int j = 0; j < 500; j++) {
                        int mgrId = rnd.nextInt(managersCnt);

                        KReplicaMapManager m = managers.get(mgrId, managersFactory);
                        try {
                            if (rnd.nextInt(1000) == 0) {
                                managers.reset(mgrId, m);
                                continue;
                            }

                            KReplicaMap<Integer,Long> map = m.getMap();

                            Integer key = rnd.nextInt(keys);
                            Long val = map.get(key);

                            if (val == null) {
                                val = map.putIfAbsent(key, 0L);
                                if (val == null)
                                    val = 0L;
                            }

                            int incr = 1 + rnd.nextInt(5);

                            long newVal = val + incr;
                            if (map.replace(key, val, newVal)) {
                                long cnt = cnts[key].addAndGet(incr);
//                                System.out.println("-->  Updated val: " + newVal + ", cnt: " + cnt);
                                assertTrue(cnt <= newVal);
                            }
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

                List<ReplicaMap<Integer,Long>> maps = new ArrayList<>();

                for (int mgrId = 0; mgrId < managersCnt; mgrId++) {
                    KReplicaMapManager m = managers.get(mgrId, managersFactory);
                    KReplicaMap<Integer,Long> map = m.getMap();

                    for (int k = 0; k < keys; k++) {
                        for (int attempt = 0; cnts[k].get() > map.getOrDefault(k, 0L); attempt++) {
                            if (attempt % 300 == 0)
                                System.out.println(Arrays.toString(cnts) + " > " + map.unwrap());
                            Thread.sleep(10);
                        }
                        maps.add(map);
                    }
                }

                awaitEqual(maps, (m1, m2) -> {
                    for (int k = 0; k < keys; k++) {
                        int c = m1.getOrDefault(k, 0L).compareTo(
                                m2.getOrDefault(k, 0L));

                        if (c != 0)
                            return c;
                    }
                    return 0;
                },60, SECONDS, m -> m.unwrap().toString());

                try (Consumer<Object,Object> dataConsumer =
                         managers.get(0, managersFactory).newKafkaConsumerData()) {
                    checkFlushedData("data", dataConsumer, offsets, false);
                }

                System.out.println("iteration " + i + " OK");
            }
        }
        finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(3, SECONDS));
        }
    }

    @SuppressWarnings({"SameParameterValue", "OptionalGetWithoutIsPresent"})
    static <X> void awaitEqual(
        List<X> list,
        Comparator<X> cmp,
        long timeout,
        TimeUnit unit,
        Function<X, String> toString
    ) throws Exception {
        long start = System.nanoTime();

        X max = list.stream().max(cmp).get();

        int lastPrintTime = 0;

        outer: for (;;) {
            for (X x : list) {
                long waitNanos = System.nanoTime() - start;
                long secsFromLastPrint = NANOSECONDS.toSeconds(waitNanos) - lastPrintTime;
                if (secsFromLastPrint >= 5) {
                    lastPrintTime += secsFromLastPrint;
                    System.err.println(list.stream().map(toString).collect(Collectors.toList()));
                }

                if (waitNanos > unit.toNanos(timeout))
                    throw new TimeoutException();

                int c = cmp.compare(max, x);

                while (c > 0) {
                    Thread.sleep(1);
                    c = cmp.compare(max, x);
                }

                if (c < 0) {
                    max = x;
                    continue outer;
                }
            }
            return;
        }
    }
}