package com.vladykin.replicamap.kafka.impl.part;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Make sure that we always partition by the key bytes.
 */
public class KeyBytesPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Utils.requireNonNull(keyBytes, "keyBytes");
        int numPartitions = cluster.partitionsForTopic(topic).size();
        return BuiltInPartitioner.partitionForKey(keyBytes, numPartitions);
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No-op
    }
}
