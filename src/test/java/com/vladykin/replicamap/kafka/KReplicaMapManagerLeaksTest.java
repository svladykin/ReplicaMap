package com.vladykin.replicamap.kafka;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerLeaksTest.CollectingMockConsumer.consumers;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerLeaksTest.CollectingMockProducer.producers;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerLeaksTest.MockReplicaMapManager.exception;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerLeaksTest.MockReplicaMapManager.failOnLoadData;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.FLUSH_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.OPS_TOPIC;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KReplicaMapManagerLeaksTest {
    @Test
    void testStart() throws Exception {
        KReplicaMapManager m = new MockReplicaMapManager();
        assertFalse(consumers.isEmpty() && producers.isEmpty());

        assertSame(m, m.start().get(3, TimeUnit.SECONDS));

        KReplicaMap<String,String> map = m.getMap();

        map.asyncPut("a", "A");
        map.asyncPut("b", "B");

        m.close();
    }

    @Test
    void testStartNoWait() {
        KReplicaMapManager m = new MockReplicaMapManager();
        m.start();
        m.close();
    }

    @Test
    void testNoStart() {
        MockReplicaMapManager m = new MockReplicaMapManager();
        m.close();
    }

    @Test
    void testFailCreate() {
        exception = new IllegalStateException();
        assertThrows(ReplicaMapException.class, MockReplicaMapManager::new);
    }

    @Test
    void testFailLoadData() {
        failOnLoadData = true;
        try (KReplicaMapManager m = new MockReplicaMapManager()) {
            assertThrows(ExecutionException.class, () -> m.start().get(3, TimeUnit.SECONDS));
        }
    }

    @BeforeEach
    void clearAll() {
        failOnLoadData = false;
        exception = null;
        producers.clear();
        consumers.clear();
    }

    @AfterEach
    void checkAllClosed() {
        assertFalse(consumers.isEmpty() && producers.isEmpty());

        for (CollectingMockProducer<?,?> producer : producers)
            assertTrue(producer.closed());

        for (CollectingMockConsumer<?,?> consumer : consumers)
            assertTrue(consumer.closed());
    }

    static class CollectingMockConsumer<K,V> extends MockConsumer<K,V> {
        static final Set<CollectingMockConsumer<?,?>> consumers = Collections.newSetFromMap(new ConcurrentHashMap<>());

        public CollectingMockConsumer() {
            super(OffsetResetStrategy.NONE);

            Map<TopicPartition,Long> m = new HashMap<>();

            m.put(new TopicPartition(DATA_TOPIC, 0), 0L);
            m.put(new TopicPartition(DATA_TOPIC, 1), 0L);

            m.put(new TopicPartition(OPS_TOPIC, 0), 0L);
            m.put(new TopicPartition(OPS_TOPIC, 1), 0L);

            m.put(new TopicPartition(FLUSH_TOPIC, 0), 0L);
            m.put(new TopicPartition(FLUSH_TOPIC, 1), 0L);

            updateEndOffsets(m);

            consumers.add(this);
        }
    }

    static class CollectingMockProducer<K,V> extends MockProducer<K,V> {
        static final Set<CollectingMockProducer<?,?>> producers = Collections.newSetFromMap(new ConcurrentHashMap<>());

        CollectingMockProducer() {
            super(new Cluster(null, new ArrayList<>(), asList(
                    new PartitionInfo(DATA_TOPIC, 0, null, null, null),
                    new PartitionInfo(DATA_TOPIC, 1, null, null, null),

                    new PartitionInfo(OPS_TOPIC, 0, null, null, null),
                    new PartitionInfo(OPS_TOPIC, 1, null, null, null),

                    new PartitionInfo(FLUSH_TOPIC, 0, null, null, null),
                    new PartitionInfo(FLUSH_TOPIC, 1, null, null, null)
                ), Collections.emptySet(), Collections.emptySet(), null)
                , false, null, null, null);

            producers.add(this);
        }
    }

    static class MockReplicaMapManager extends KReplicaMapManager {
        static RuntimeException exception;
        static boolean failOnLoadData;

        public MockReplicaMapManager() {
            super(new HashMap<>());
        }

        @Override
        protected void onNewReplicaMapManager() {
            if (exception != null)
                throw exception;
        }

        @Override
        protected Consumer<Object,Object> newKafkaConsumerData() {
            return new CollectingMockConsumer<>();
        }

        @Override
        protected Consumer<Object,OpMessage> newKafkaConsumerOps() {
            CollectingMockConsumer<Object,OpMessage> c = new CollectingMockConsumer<>();

            if (failOnLoadData)
                c.setException(new KafkaException("ops"));

            return c;
        }

        @Override
        protected Consumer<Object,OpMessage> newKafkaConsumerFlush() {
            return new CollectingMockConsumer<>();
        }

        @Override
        protected Producer<Object,Object> newKafkaProducerData(int part) {
            return new CollectingMockProducer<>();
        }

        @Override
        protected Producer<Object,OpMessage> newKafkaProducerFlush() {
            return new CollectingMockProducer<>();
        }

        @Override
        protected Producer<Object,OpMessage> newKafkaProducerOps() {
            return new CollectingMockProducer<>();
        }
    }
}