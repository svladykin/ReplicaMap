package com.vladykin.replicamap.kafka.impl.part;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

/**
 * Make sure that we always partition by the key bytes.
 */
public class KeyBytesPartitioner extends DefaultPartitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Utils.requireNonNull(keyBytes, "keyBytes");
        return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
    }
}
