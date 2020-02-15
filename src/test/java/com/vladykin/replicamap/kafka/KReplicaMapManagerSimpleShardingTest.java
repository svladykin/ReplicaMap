package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.ALLOWED_PARTITIONS;
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

class KReplicaMapManagerSimpleShardingTest {
    static final int START_TIMEOUT = 60;

    static final int PARTS = 4;

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    Map<String,Object> getShardedConfig(String allowedParts) {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));
        cfg.put(FLUSH_PERIOD_OPS, 2);
        cfg.put(FLUSH_MAX_POLL_TIMEOUT_MS, 1L);

        cfg.put(KEY_SERIALIZER_CLASS, IntegerSerializer.class);
        cfg.put(KEY_DESERIALIZER_CLASS, IntegerDeserializer.class);

        cfg.put(VALUE_SERIALIZER_CLASS, IntegerSerializer.class);
        cfg.put(VALUE_DESERIALIZER_CLASS, IntegerDeserializer.class);

        cfg.put(PARTITIONER_CLASS, IntPartitioner.class);

        if (allowedParts != null)
            cfg.put(ALLOWED_PARTITIONS, Arrays.asList(allowedParts.split(",")));

        return cfg;
    }

    @Test
    void testSimpleSharding() throws Exception {
        createTopics(sharedKafkaTestResource,
            DEFAULT_DATA_TOPIC,
            DEFAULT_DATA_TOPIC + DEFAULT_OPS_TOPIC_SUFFIX,
            DEFAULT_DATA_TOPIC + DEFAULT_FLUSH_TOPIC_SUFFIX,
            PARTS);

        KReplicaMapManager all = new KReplicaMapManager(getShardedConfig(null));

        all.start(START_TIMEOUT, TimeUnit.SECONDS);

        for (int i = 0; i < 10; i++)
            all.getMap().put(i, 0);

        KReplicaMapManager shard1 = new KReplicaMapManager(getShardedConfig("0,3"));
        KReplicaMapManager shard2 = new KReplicaMapManager(getShardedConfig("1,2"));
        KReplicaMapManager shard3 = new KReplicaMapManager(getShardedConfig("1,3"));
        KReplicaMapManager shard4 = new KReplicaMapManager(getShardedConfig("0,2"));

        CompletableFuture.allOf(
            shard1.start(),
            shard2.start(),
            shard3.start(),
            shard4.start()
        ).get(START_TIMEOUT, TimeUnit.SECONDS);

        assertEquals(new HashSet<>(Arrays.asList(0,4,8,3,7)), shard1.getMap().keySet());
        assertEquals(new HashSet<>(Arrays.asList(1,5,9,2,6)), shard2.getMap().keySet());
        assertEquals(new HashSet<>(Arrays.asList(1,5,9,3,7)), shard3.getMap().keySet());
        assertEquals(new HashSet<>(Arrays.asList(0,4,8,2,6)), shard4.getMap().keySet());
        assertEquals(new HashSet<>(Arrays.asList(0,1,2,3,4,5,6,7,8,9)), all.getMap().keySet());
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
}