package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMapException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.ALLOWED_PARTITIONS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.ALLOWED_PARTITIONS_RESOLVER;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_FLUSH_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_OPS_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_MAX_POLL_TIMEOUT_MS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_PERIOD_OPS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.KEY_DESERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.KEY_SERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.PARTITIONER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.VALUE_DESERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.VALUE_SERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KReplicaMapManagerSimpleShardingTest {
    static final int START_TIMEOUT = 60;

    static final int PARTS = 4;

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    Map<String,Object> getShardedConfig(String allowedParts, boolean useResolver) {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));
        cfg.put(FLUSH_PERIOD_OPS, 2);
        cfg.put(FLUSH_MAX_POLL_TIMEOUT_MS, 1L);

        cfg.put(KEY_SERIALIZER_CLASS, IntegerSerializer.class);
        cfg.put(KEY_DESERIALIZER_CLASS, IntegerDeserializer.class);

        cfg.put(VALUE_SERIALIZER_CLASS, IntegerSerializer.class);
        cfg.put(VALUE_DESERIALIZER_CLASS, IntegerDeserializer.class);

        cfg.put(PARTITIONER_CLASS, IntPartitioner.class);

        if (allowedParts != null) {
            if (useResolver) {
                cfg.put(ALLOWED_PARTITIONS_RESOLVER, AllowedPartsResolver.class);
                cfg.put(AllowedPartsResolver.ALLOWED_PARTS, allowedParts);
            }
            else
                cfg.put(ALLOWED_PARTITIONS, parseAllowedParts(allowedParts));
        }

        return cfg;
    }

    static List<String> parseAllowedParts(String allowedParts) {
        return Arrays.asList(allowedParts.split(","));
    }

    @Test
    void testSimpleSharding() throws Exception {
        createTopics(sharedKafkaTestResource,
            DEFAULT_DATA_TOPIC,
            DEFAULT_DATA_TOPIC + DEFAULT_OPS_TOPIC_SUFFIX,
            DEFAULT_DATA_TOPIC + DEFAULT_FLUSH_TOPIC_SUFFIX,
            PARTS);

        KReplicaMapManager all = new KReplicaMapManager(getShardedConfig(null, false));

        all.start(Duration.ofSeconds(START_TIMEOUT));

        for (int i = 0; i < 12; i++)
            all.getMap().put(i, 0);

        awaitForFlush(all);

        KReplicaMapManager shard1 = new KReplicaMapManager(getShardedConfig("0,3", false));
        KReplicaMapManager shard2 = new KReplicaMapManager(getShardedConfig("1,2", true));
        KReplicaMapManager shard3 = new KReplicaMapManager(getShardedConfig("1,3", false));
        KReplicaMapManager shard4 = new KReplicaMapManager(getShardedConfig("0,2", true));

        CompletableFuture.allOf(
            shard1.start(),
            shard2.start(),
            shard3.start(),
            shard4.start()
        ).get(START_TIMEOUT, TimeUnit.SECONDS);

        assertEquals(2, AllowedPartsResolver.cnt.get());

        assertEquals(new HashSet<>(Arrays.asList(0,4,8,3,7,11)), shard1.getMap().keySet());
        assertEquals(new HashSet<>(Arrays.asList(1,5,9,2,6,10)), shard2.getMap().keySet());
        assertEquals(new HashSet<>(Arrays.asList(1,5,9,3,7,11)), shard3.getMap().keySet());
        assertEquals(new HashSet<>(Arrays.asList(0,4,8,2,6,10)), shard4.getMap().keySet());
        assertEquals(new HashSet<>(Arrays.asList(0,1,2,3,4,5,6,7,8,9,10,11)), all.getMap().keySet());

        all.stop(); // To make sure that only shards actually flush the data.

        assertEquals(0, shard1.getMap().put(3, 1));
        assertThrows(ReplicaMapException.class, () -> shard1.getMap().put(1, 1));
        assertEquals(0, shard2.getMap().put(2, 1));
        assertThrows(ReplicaMapException.class, () -> shard2.getMap().put(3, 1));
        assertEquals(0, shard3.getMap().put(1, 1));
        assertThrows(ReplicaMapException.class, () -> shard3.getMap().put(0, 1));
        assertEquals(0, shard4.getMap().put(0, 1));
        assertThrows(ReplicaMapException.class, () -> shard4.getMap().put(5, 1));

        awaitForFlushRequests(4, shard1, shard2, shard3, shard4);
        awaitForFlush(shard1, shard2, shard3, shard4);
    }

    @SuppressWarnings("SameParameterValue")
    void awaitForFlushRequests(long flushReqs, KReplicaMapManager... ms) throws InterruptedException, TimeoutException {
        long start = System.nanoTime();

        for (;;) {
            int total = 0;

            for (KReplicaMapManager m : ms)
                total += m.getReceivedFlushRequests();

            if (flushReqs == total)
                break;

            Thread.sleep(20);

            if (System.nanoTime() - start > TimeUnit.SECONDS.toNanos(30))
                throw new TimeoutException();
        }
    }

    void awaitForFlush(KReplicaMapManager... ms) throws InterruptedException, TimeoutException {
        long start = System.nanoTime();

        for (;;) {
            int total = 0;

            for (KReplicaMapManager m : ms)
                total += m.getSuccessfulFlushes();

            if (total > 0)
                break;

            Thread.sleep(20);

            if (System.nanoTime() - start > TimeUnit.SECONDS.toNanos(30))
                throw new TimeoutException();
        }
    }

    public static class IntPartitioner implements Partitioner {

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return (int)key % PARTS;
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public void configure(Map<String,?> configs) {
            // no-op
        }
    }

    public static class AllowedPartsResolver implements Supplier<Set<Integer>>, Configurable {

        static final AtomicInteger cnt = new AtomicInteger();

        static final String ALLOWED_PARTS = AllowedPartsResolver.class.getName() + ".allowedParts";

        Set<Integer> allowedParts;

        @Override
        public void configure(Map<String,?> configs) {
            String allowedPartsString = (String)configs.get(ALLOWED_PARTS);

            allowedParts = parseAllowedParts(allowedPartsString)
                .stream()
                .map(Integer::parseInt)
                .collect(Collectors.toSet());
        }

        @Override
        public Set<Integer> get() {
            assertNotNull(allowedParts);
            cnt.incrementAndGet();
            return allowedParts;
        }
    }
}