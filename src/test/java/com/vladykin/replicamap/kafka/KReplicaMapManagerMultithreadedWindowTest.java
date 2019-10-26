package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
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
import static com.vladykin.replicamap.kafka.KReplicaMapManagerMultithreadedIncrementRestartTest.awaitEqual;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerMultithreadedIncrementSimpleTest.checkFlushedData;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KReplicaMapManagerMultithreadedWindowTest {
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    Map<String,Object> getDefaultConfig() {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));

        cfg.put(FLUSH_PERIOD_OPS, 20);
        cfg.put(FLUSH_MIN_OPS, 15);

        cfg.put(DATA_TOPIC, "data");
        cfg.put(OPS_TOPIC, "ops");
        cfg.put(FLUSH_TOPIC, "flush");

        cfg.put(OPS_WORKERS, 3);
        cfg.put(FLUSH_WORKERS, 2);

        cfg.put(MAPS_HOLDER, KReplicaMapManagerMultithreadedIncrementSimpleTest.SkipListMapHolder.class);

        cfg.put(KEY_SERIALIZER_CLASS, LongSerializer.class);
        cfg.put(KEY_DESERIALIZER_CLASS, LongDeserializer.class);

        cfg.put(VALUE_SERIALIZER_CLASS, LongSerializer.class);
        cfg.put(VALUE_DESERIALIZER_CLASS, LongDeserializer.class);

        return cfg;
    }

    @Test
    void testMultithreadedSlidingWindowWithRestart() throws Exception {
        int threadsCnt = 13;
        int managersCnt = 5;
        int parts = 4;

        createTopics(sharedKafkaTestResource.getKafkaTestUtils().getAdminClient(),
            "data", "ops", "flush", parts);

        LazyList<KReplicaMapManager> managers = new LazyList<>(managersCnt);
        IntFunction<KReplicaMapManager> managersFactory =  (x) -> {
            KReplicaMapManager manager = new KReplicaMapManager(getDefaultConfig());
            manager.start(120, SECONDS);
            System.out.println("started: " + x);
            return manager;
        };

        AtomicLong[] lastAddedKey = new AtomicLong[threadsCnt];
        KReplicaMapManager mgr = managers.get(0, managersFactory);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        try (Consumer<Object,Object> dataConsumer = mgr.newKafkaConsumerData()) {
            checkFlushedData("data", dataConsumer, offsets, true);
        }

        for (long k = 0; k < threadsCnt; k++) {
            mgr.getMap().put(k, -k);
            lastAddedKey[(int)k] = new AtomicLong(k);
        }
        assertEquals(threadsCnt, mgr.getMap().size());
        assertTrue(managers.reset(0, mgr));

        ExecutorService exec = Executors.newFixedThreadPool(threadsCnt);

        try {
            for (int i = 0; i < 10; i++) {
                AtomicInteger threadIds = new AtomicInteger();
                CyclicBarrier start = new CyclicBarrier(threadsCnt);

                CompletableFuture<?> fut = Utils.allOf(executeThreads(threadsCnt, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();
                    int threadId = threadIds.getAndIncrement();

                    start.await();

                    for (int j = 0; j < 500; j++) {
                        int mgrId = rnd.nextInt(managersCnt);

                        KReplicaMapManager m = managers.get(mgrId, managersFactory);
                        try {
                            if (rnd.nextInt(1000) == 0) {
                                managers.reset(mgrId, m);
                                continue;
                            }
                            KReplicaNavigableMap<Long,Long> map = (KReplicaNavigableMap<Long,Long>)m.<Long,Long>getMap();

                            long delKey = lastAddedKey[threadId].get();
                            long addKey = delKey + threadsCnt;

                            Long delVal = map.remove(delKey);
                            if (delVal != null)
                                assertEquals(-delKey, delVal);

                            map.putIfAbsent(addKey, -addKey);

                            assertTrue(lastAddedKey[threadId].compareAndSet(delKey, addKey));
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

                List<KReplicaMap<Long,Long>> maps = new ArrayList<>();

                for (int mgrId = 0; mgrId < managersCnt; mgrId++) {
                    KReplicaMapManager m = managers.get(mgrId, managersFactory);
                    maps.add(m.getMap());
                }

                awaitEqual(maps, (m1, m2) -> {
                    Iterator<Long> it1 = m1.keySet().iterator();
                    Iterator<Long> it2 = m2.keySet().iterator();

                    for (;;) {
                        if (it1.hasNext()) {
                            if (!it2.hasNext())
                                break;
                        }
                        else if (it2.hasNext())
                            break;
                        else
                            return 0;

                        long k1 = it1.next();
                        long k2 = it2.next();

                        if (k1 != k2)
                            break;
                    }

                    // Here we do not actually know which one is greater, we only know they are unequal.
                    // If we provide a consistent comparator, we may hang on a wrong maximum.
                    // Thus we randomize, so that we always make progress.
                    return ThreadLocalRandom.current().nextBoolean() ? -1 : 1;
                }, 120, SECONDS, m -> m.unwrap().toString());

                try (Consumer<Object,Object> dataConsumer =
                         managers.get(0, managersFactory).newKafkaConsumerData()) {
                    checkFlushedData("data", dataConsumer, offsets, false);
                }

                System.out.println("iteration " + i + " OK");
            }
        }
        finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(10, SECONDS));
        }
    }

}
