package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
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
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_MAX_PARALLEL;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_WORKERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.VALUE_DESERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.VALUE_SERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KReplicaMapManagerMultithreadedPutSimpleTest {

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    Map<String,Object> getDefaultConfig() {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));

        cfg.put(FLUSH_PERIOD_OPS, 50);

        cfg.put(DATA_TOPIC, "data");
        cfg.put(OPS_TOPIC, "ops");
        cfg.put(FLUSH_TOPIC, "flush");

        cfg.put(OPS_MAX_PARALLEL, 3000);

        cfg.put(OPS_WORKERS, 4);
        cfg.put(FLUSH_WORKERS, 2);

        cfg.put(KEY_SERIALIZER_CLASS, IntegerSerializer.class);
        cfg.put(KEY_DESERIALIZER_CLASS, IntegerDeserializer.class);

        cfg.put(VALUE_SERIALIZER_CLASS, LongSerializer.class);
        cfg.put(VALUE_DESERIALIZER_CLASS, LongDeserializer.class);

        return cfg;
    }

    @Test
    void testMultithreadedAsyncPut() throws Exception {
        int threadsCnt = 10;
        int managersCnt = 3;
        int keys = 100_000;
        int parts = 4;

        createTopics(sharedKafkaTestResource,
            "data", "ops", "flush", parts);

        LazyList<KReplicaMapManager> managers = new LazyList<>(managersCnt);
        IntFunction<KReplicaMapManager> managersFactory =  (x) -> {
            KReplicaMapManager manager = new KReplicaMapManager(getDefaultConfig());
            try {
                manager.start().get(300, SECONDS);
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
            System.out.println("started: " + x);
            return manager;
        };

        AtomicLong cnt = new AtomicLong();

        ExecutorService exec = Executors.newFixedThreadPool(threadsCnt);

        try {
            for (int i = 0; i < 10; i++) {
                CyclicBarrier start = new CyclicBarrier(threadsCnt);

                long startTime = System.nanoTime();

                CompletableFuture<?> fut = Utils.allOf(executeThreads(threadsCnt, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    List<CompletableFuture<?>> futs = new ArrayList<>();

                    start.await();

                    for (int j = 0; j < 1000; j++) {
                        int mgrId = rnd.nextInt(managersCnt);

                        KReplicaMapManager m = managers.get(mgrId, managersFactory);
                        KReplicaMap<Integer,Long> map = m.getMap();

                        Integer key = rnd.nextInt(keys);
                        futs.add(map.asyncPut(key, rnd.nextLong()));
//                        map.put(key, rnd.nextLong());
                        cnt.incrementAndGet();

//                        if (j % 50 == 0)
//                            System.out.println(j);
                    }

                    Utils.allOf(futs).get();

                    return null;
                }));

                fut.get(180, SECONDS);

                long time = NANOSECONDS.toMillis(System.nanoTime() - startTime);
                System.out.println("iteration " + i + " OK, time: " + time + "ms, cnt: " + cnt.getAndSet(0));
            }
        }
        finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(10, SECONDS));
            Utils.close(managers);
        }
    }
}