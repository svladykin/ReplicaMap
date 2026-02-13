package com.vladykin.replicamap.kafka.impl.part;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * To make sure that partitions are always set explicitly.
 *
 * @author Sergei Vladykin http://vladykin.com
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
