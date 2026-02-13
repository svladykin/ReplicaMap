package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.holder.MapsHolderSingle;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

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
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class KReplicaMapManagerMultithreadedIncrementSimpleTest {

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

        cfg.put(MAPS_HOLDER, SkipListMapHolder.class);

        cfg.put(KEY_SERIALIZER_CLASS, IntegerSerializer.class);
        cfg.put(KEY_DESERIALIZER_CLASS, IntegerDeserializer.class);

        cfg.put(VALUE_SERIALIZER_CLASS, LongSerializer.class);
        cfg.put(VALUE_DESERIALIZER_CLASS, LongDeserializer.class);

        return cfg;
    }

    @SuppressWarnings("BusyWait")
    @Test
    void testMultithreadedIncrement() throws Exception {
        int threadsCnt = 25;
        int managersCnt = 7;
        int keys = 9;
        int parts = 4;

        createTopics(sharedKafkaTestResource,
            "data", "ops", "flush", parts);

        LazyList<KReplicaMapManager> managers = new LazyList<>(managersCnt);
        IntFunction<KReplicaMapManager> managersFactory =  (x) -> {
            KReplicaMapManager manager = new KReplicaMapManager(getDefaultConfig());
            manager.start(300, SECONDS);
            System.out.println("started: " + x);
            return manager;
        };

        AtomicLong[] cnts = new AtomicLong[keys];
        for (int i = 0; i < keys; i++)
            cnts[i] = new AtomicLong();

        Map<TopicPartition, Long> offsets = new HashMap<>();
        try (Consumer<Object,Object> dataConsumer =
                 managers.get(0, managersFactory).newKafkaConsumerData()) {
            awaitFlushedData("data", dataConsumer, offsets, true);
        }

        ExecutorService exec = Executors.newFixedThreadPool(threadsCnt);

        try {
            for (int i = 0; i < 5; i++) {
                CyclicBarrier start = new CyclicBarrier(threadsCnt);

                CompletableFuture<?> fut = Utils.allOf(executeThreads(threadsCnt, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    start.await();

                    for (int j = 0; j < 500; j++) {
                        int mgrId = rnd.nextInt(managersCnt);

                        KReplicaMapManager m = managers.get(mgrId, managersFactory);
                        KReplicaMap<Integer,Long> map = m.getMap();

                        Integer key = rnd.nextInt(keys);
                        Long val = map.get(key);

                        if (val == null) {
                            val = map.putIfAbsent(key, 0L);
                            if (val == null)
                                val = 0L;
                        }

                        int incr = 1 + rnd.nextInt(10);

                        if (map.replace(key, val, val + incr))
                            cnts[key].addAndGet(incr);

//                        if (j % 50 == 0)
//                            System.out.println(j);
                    }

                    return null;
                }));

                fut.get(180, SECONDS);

                System.out.println("checking " + i);

                for (int mgrId = 0; mgrId < managersCnt; mgrId++) {
                    KReplicaMapManager m = managers.get(mgrId, managersFactory);
//                    System.out.println(mgrId + " -> flushWrk:  " + m.getFlushWorkers());
//                    System.out.println(mgrId + " -> updates:   " + m.getReceivedUpdates());
//                    System.out.println(mgrId + " -> flushReqs: " + m.getSentFlushRequests());
//                    System.out.println(mgrId + " -> flushReqs: " + m.getReceivedFlushRequests());
//                    System.out.println(mgrId + " -> flushes:   " + m.getSuccessfulFlushes());
//                    System.out.println();

                    KReplicaMap<Integer,Long> map = m.getMap();

                    for (int k = 0; k < keys; k++) {
                        for (int attempt = 0; cnts[k].get() != map.getOrDefault(k, 0L) ; attempt++) {
                            if (attempt % 3000 == 0)
                                System.out.println(Arrays.toString(cnts) + " <> " + map.unwrap());
                            Thread.sleep(1);
                        }
                    }
                }

                try (Consumer<Object,Object> dataConsumer =
                         managers.get(0, managersFactory).newKafkaConsumerData()) {
                    awaitFlushedData("data", dataConsumer, offsets, false);
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

    @SuppressWarnings("BusyWait")
    static void awaitFlushedData(
        String dataTopic,
        Consumer<Object,Object> dataConsumer,
        Map<TopicPartition, Long> offsets,
        boolean preCheck
    ) throws Exception {
        boolean ok = preCheck;
        long start = System.nanoTime();

        for (;;) {
            for (PartitionInfo partInfo : dataConsumer.partitionsFor(dataTopic)) {
                TopicPartition part = new TopicPartition(dataTopic, partInfo.partition());
                Long oldOffset = offsets.get(part);
                long newOffset = Utils.endOffset(dataConsumer, part);

                if (oldOffset != null) {
                    if (newOffset == oldOffset)
                        continue;

                    assertTrue(newOffset > oldOffset,
                        newOffset + " > " + oldOffset + " for partition " + part);

                    dataConsumer.assign(singleton(part));
                    dataConsumer.seek(part, oldOffset);

                    long miniStart = System.nanoTime();
                    Duration pollTimeout = Duration.ofSeconds(1);

                    for (;;) {
                        ConsumerRecords<Object,Object> data = Utils.poll(dataConsumer, pollTimeout);

                        if (!data.isEmpty())
                            break;

                        if (System.nanoTime() - miniStart > SECONDS.toNanos(15))
                            fail("Failed to fetch committed data for partition " + part);
                    }

                    ok = true;
                }
                else
                    assertEquals(0L, newOffset);

                offsets.put(part, newOffset);
            }

            if (ok)
                return;

            if (System.nanoTime() - start > SECONDS.toNanos(60))
                throw new TimeoutException();

            Thread.sleep(100);
        }
    }

    public static class SkipListMapHolder extends MapsHolderSingle {
        @SuppressWarnings("SortedCollectionWithNonComparableKeys")
        @Override
        protected <K, V> Map<K,V> createInnerMap() {
            return new ConcurrentSkipListMap<>();
        }
    }
}