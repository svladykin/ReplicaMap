package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_FLUSH_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_OPS_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_MAX_UNFLUSHED_MS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_PERIOD_OPS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerFailureTest.awaitOrFail;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The time-based flush trigger must flush an idle partition's tail to the {@code data} topic even when
 * no update ever crosses a {@code flush.period.ops} boundary, so external {@code data}-topic readers
 * eventually see every update.
 */
class KReplicaMapIdleFlushTest {

    static final String DATA = DEFAULT_DATA_TOPIC;
    static final String OPS = DEFAULT_DATA_TOPIC + DEFAULT_OPS_TOPIC_SUFFIX;
    static final String FLUSH = DEFAULT_DATA_TOPIC + DEFAULT_FLUSH_TOPIC_SUFFIX;

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    Map<String,Object> getConfig() {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));
        cfg.put(FLUSH_PERIOD_OPS, 1_000_000); // So the offset-based trigger can never fire here.
        cfg.put(FLUSH_MAX_UNFLUSHED_MS, 300L); // Force a flush quickly.
        return cfg;
    }

    @Test
    void testIdleUpdateReachesDataTopic() throws Exception {
        createTopics(sharedKafkaTestResource, DATA, OPS, FLUSH, 4);

        try (KReplicaMapManager m = new KReplicaMapManager(getConfig())) {
            m.start(60, SECONDS);

            KReplicaMap<String,String> map = m.getMap();

            // A single update, nowhere near flush.period.ops — only the time-based trigger can flush it.
            map.put("k", "v");

            // Read the data topic directly: this proves the value actually reached `data`, not just `ops`.
            assertEquals("v", awaitDataTopicValue(sharedKafkaTestResource, DATA, "k", 30));
            assertTrue(m.getSuccessfulFlushes() >= 1, "Expected at least one flush");
        }
    }

    /**
     * Only the flush-partition owner may run the time-based trigger, otherwise every client in the
     * cluster sends the same request for the same idle tail. And when the owner goes away, the
     * survivor must pick the partition up and keep flushing it — the ownership handoff is what makes
     * the trigger survive a restart, and nothing else covers it.
     */
    @Test
    void testIdleFlushIsOwnedByOneClientAndHandedOver() throws Exception {
        String data = "idle_owner";
        // A single partition, so there is exactly one owner to reason about.
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 1);

        Map<String,Object> cfg = getConfig();
        cfg.put(DATA_TOPIC, data);
        cfg.put(OPS_TOPIC, data + "_ops");
        cfg.put(FLUSH_TOPIC, data + "_flush");

        KReplicaMapManager m1 = new KReplicaMapManager(cfg);
        KReplicaMapManager m2 = new KReplicaMapManager(cfg);

        try {
            m1.start(60, SECONDS);
            m2.start(60, SECONDS);

            // Only update once the flush group settled, otherwise a rebalance could hand the
            // partition over mid-flight and both clients would legitimately send a request.
            awaitOrFail(SECONDS.toNanos(60), () -> {
                int a1 = m1.getAssignedFlushPartitions();
                int a2 = m2.getAssignedFlushPartitions();

                return a1 + a2 == 1 ? null : "Flush ownership is not settled yet: " + a1 + " and " + a2;
            });

            m1.<String,String,KReplicaMap<String,String>>getMap().put("k1", "v1");

            assertEquals("v1", awaitDataTopicValue(sharedKafkaTestResource, data, "k1", 30));

            // Only the owner asked for it: flush.period.ops is huge here, so every flush request
            // counted is a time-based one.
            long sent1 = m1.getSentFlushRequests();
            long sent2 = m2.getSentFlushRequests();

            assertTrue(sent1 == 0 || sent2 == 0,
                "Both clients requested the idle flush (a request herd): " + sent1 + " and " + sent2);
            assertTrue(sent1 + sent2 > 0, "Nobody requested the idle flush");

            // Stop the owner and let the survivor take the flush partition over.
            KReplicaMapManager owner = sent1 > 0 ? m1 : m2;
            KReplicaMapManager survivor = owner == m1 ? m2 : m1;

            owner.close();

            awaitOrFail(SECONDS.toNanos(60), () -> survivor.getAssignedFlushPartitions() == 1 ?
                null : "The survivor did not take the flush partition over");

            // A brand new idle update must still reach the data topic, now flushed by the survivor.
            survivor.<String,String,KReplicaMap<String,String>>getMap().put("k2", "v2");

            assertEquals("v2", awaitDataTopicValue(sharedKafkaTestResource, data, "k2", 60));
            assertTrue(survivor.getSentFlushRequests() > 0, "The new owner never requested a flush");
        }
        finally {
            m1.close();
            m2.close();
        }
    }

    /** Awaits the latest committed value for the key readable from the data topic by an independent
     * consumer, or {@code null} on timeout. */
    static String awaitDataTopicValue(SharedKafkaTestResource kafka, String dataTopic, String key, long timeoutSeconds) {
        Map<String,Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getKafkaConnectString());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "data-topic-test-reader");
        cfg.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String,String> dataConsumer =
                 new KafkaConsumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
            List<TopicPartition> parts = dataConsumer.partitionsFor(dataTopic).stream()
                .map(PartitionInfo::partition)
                .map(p -> new TopicPartition(dataTopic, p))
                .collect(Collectors.toList());

            dataConsumer.assign(parts);
            dataConsumer.seekToBeginning(parts);

            String value = null;
            long deadline = System.nanoTime() + SECONDS.toNanos(timeoutSeconds);

            while (value == null && System.nanoTime() < deadline) {
                ConsumerRecords<String,String> recs = dataConsumer.poll(Duration.ofMillis(200));

                for (ConsumerRecord<String,String> rec : recs) {
                    if (key.equals(rec.key()))
                        value = rec.value(); // Keep the latest value for the key.
                }
            }

            return value;
        }
    }
}
