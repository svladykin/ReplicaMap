package com.vladykin.replicamap.kafka.impl.part;

import com.vladykin.replicamap.ReplicaMapException;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AllowedOnlyPartitionerTest {
    @Test
    void testAllowedOnlyPartitioner() {
        short[] allowedParts = {1, 3, 7};

        AllowedOnlyPartitioner p = new AllowedOnlyPartitioner();

        Map<String,Object> cfg = new HashMap<>();
        AllowedOnlyPartitioner.setupProducerConfig(cfg, allowedParts, IntPartitioner.class);

        assertEquals(0, IntPartitioner.configured.get());
        p.configure(cfg);
        assertEquals(1, IntPartitioner.configured.get());

        assertEquals(allowedParts, p.allowedParts);
        assertSame(p.delegate.getClass(), IntPartitioner.class);

        assertEquals(1, p.partition(null, 1, null, null, null, null));
        assertEquals(3, p.partition(null, 3, null, null, null, null));
        assertEquals(7, p.partition(null, 7, null, null, null, null));

        assertThrows(ReplicaMapException.class, () ->
            p.partition(null, 0, null, null, null, null));
        assertThrows(ReplicaMapException.class, () ->
            p.partition(null, 2, null, null, null, null));
        assertThrows(ReplicaMapException.class, () ->
            p.partition(null, 4, null, null, null, null));
        assertThrows(ReplicaMapException.class, () ->
            p.partition(null, 5, null, null, null, null));
        assertThrows(ReplicaMapException.class, () ->
            p.partition(null, 6, null, null, null, null));
        assertThrows(ReplicaMapException.class, () ->
            p.partition(null, 8, null, null, null, null));

        assertEquals(0, IntPartitioner.closed.get());
        p.close();
        assertEquals(1, IntPartitioner.closed.get());
    }

    public static class IntPartitioner implements Partitioner {
        static final AtomicInteger configured = new AtomicInteger();
        static final AtomicInteger closed = new AtomicInteger();

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return (int)key;
        }

        @Override
        public void close() {
            closed.incrementAndGet();
        }

        @Override
        public void configure(Map<String,?> configs) {
            configured.incrementAndGet();
        }
    }
}