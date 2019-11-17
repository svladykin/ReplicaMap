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

    static final String TOPIC_SUFFIX = "_zzz";
    static final String DATA = "data" + TOPIC_SUFFIX;
    static final String OPS = "ops" + TOPIC_SUFFIX;
    static final String FLUSH = "flush" + TOPIC_SUFFIX;

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    Map<String,Object> getDefaultConfig() {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));

        cfg.put(FLUSH_PERIOD_OPS, 20);

        cfg.put(DATA_TOPIC, DATA);
        cfg.put(OPS_TOPIC, OPS);
        cfg.put(FLUSH_TOPIC, FLUSH);

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
        int threadsCnt = 31;
        int managersCnt = 9;
        int parts = 4;

        createTopics(sharedKafkaTestResource,
            DATA, OPS, FLUSH, parts);

        LazyList<KReplicaMapManager> managers = new LazyList<>(managersCnt);
        IntFunction<KReplicaMapManager> managersFactory =  (mgrId) -> {
            KReplicaMapManager manager = new KReplicaMapManager(getDefaultConfig());
            manager.start(120, SECONDS);
            System.out.println("started: " + mgrId + " " +  Long.toHexString(manager.clientId));
            return manager;
        };

        AtomicLong[] lastAddedKey = new AtomicLong[threadsCnt];
        KReplicaMapManager mgr = managers.get(0, managersFactory);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        try (Consumer<Object,Object> dataConsumer = mgr.newKafkaConsumerData()) {
            checkFlushedData(DATA, dataConsumer, offsets, true);
        }

        for (long k = 0; k < threadsCnt; k++) {
            mgr.getMap().put(k, 1L);
            lastAddedKey[(int)k] = new AtomicLong(k);
        }
        assertEquals(threadsCnt, mgr.getMap().size());

        ExecutorService exec = Executors.newFixedThreadPool(threadsCnt);

        try {
            for (int i = 0; i < 10; i++) {
                AtomicInteger threadIds = new AtomicInteger();
                CyclicBarrier start = new CyclicBarrier(threadsCnt);

                CompletableFuture<?> fut = Utils.allOf(executeThreads(threadsCnt, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();
                    int threadId = threadIds.getAndIncrement();
                    int mgrId = rnd.nextInt(managersCnt);

                    start.await();

                    for (int j = 0; j < 500; j++) {
                        KReplicaMapManager m = managers.get(mgrId, managersFactory);
                        try {
                            // Periodically restart managers, but keep one always running.
                            if (mgrId != 0 && rnd.nextInt(1000) == 0) {
                                System.out.println("stopping: " + mgrId + " " +  Long.toHexString(m.clientId));
                                managers.reset(mgrId, m);
                                continue;
                            }
                            KReplicaNavigableMap<Long,Long> map = (KReplicaNavigableMap<Long,Long>)m.<Long,Long>getMap();

                            long delKey = lastAddedKey[threadId].get();
                            long addKey = delKey + threadsCnt;

                            Long delVal = map.remove(delKey);
                            if (delVal != null)
                                assertEquals(1L, delVal);

                            map.putIfAbsent(addKey, 1L);

                            assertTrue(lastAddedKey[threadId].compareAndSet(delKey, addKey));

//                            if (j % 10 == 0)
//                                System.out.println(map.unwrap());
                        }
                        catch (ReplicaMapException e) {
                            // may happen if another thread concurrently closed our manager
                        }
                    }

                    return null;
                }));

                fut.get(300, SECONDS);

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
                }, 120, SECONDS, m -> "\n" + Long.toHexString(m.getManager().clientId) + "  " + m.unwrap().toString());

                try (Consumer<Object,Object> dataConsumer =
                         managers.get(0, managersFactory).newKafkaConsumerData()) {
                    checkFlushedData(DATA, dataConsumer, offsets, false);
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

}
