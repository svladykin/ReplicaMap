package com.vladykin.replicamap.kafka.impl.part;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * To make sure that partitions are always set explicitly.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class NeverPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        throw new IllegalStateException("Must never be called.");
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
