package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.vladykin.replicamap.kafka.KReplicaMapIdleFlushTest.awaitDataTopicValue;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_MAX_UNFLUSHED_MS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_PERIOD_OPS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.NO_LIMIT;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real-broker validation of the startup resume over retention-deleted ops ranges: retention is
 * emulated deterministically with {@code AdminClient.deleteRecords} (raises the real log start,
 * producing real {@code OffsetOutOfRangeException} behavior under {@code auto.offset.reset=none}).
 * Single-partition topics keep the offsets deterministic.
 */
class KReplicaMapOpsRetentionTest {

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    Map<String,Object> getConfig(String dataTopic, long maxUnflushedMs) {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));
        cfg.put(DATA_TOPIC, dataTopic);
        cfg.put(OPS_TOPIC, dataTopic + "_ops");
        cfg.put(FLUSH_TOPIC, dataTopic + "_flush");
        cfg.put(FLUSH_PERIOD_OPS, 1_000_000); // The offset-based flush trigger can never fire here.
        cfg.put(FLUSH_MAX_UNFLUSHED_MS, maxUnflushedMs);
        return cfg;
    }

    @Test
    void testRestartAfterAllOpsDeleted() throws Exception {
        // A fully-flushed partition idle past the ops retention: ALL ops records (including the flush
        // notifications) are gone, log start == head. A fresh client must still start cleanly from the
        // data-topic snapshot (this crashed with OffsetOutOfRangeException before the resume rework).
        String data = "ret_a";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 1);

        try (KReplicaMapManager m = new KReplicaMapManager(getConfig(data, 300))) {
            m.start(60, SECONDS);
            KReplicaMap<String,String> map = m.getMap();

            map.put("k1", "v1");
            map.put("k2", "v2");
            map.put("k3", "v3");

            // All values in the data topic => flushed, notifications written.
            for (int i = 1; i <= 3; i++)
                assertEquals("v" + i, awaitDataTopicValue(sharedKafkaTestResource, data, "k" + i, 30));
        }

        TopicPartition opsPart = new TopicPartition(data + "_ops", 0);
        deleteOpsRecords(opsPart, endOffset(opsPart)); // Everything aged out: log start == head.

        try (KReplicaMapManager m2 = new KReplicaMapManager(getConfig(data, 300))) {
            m2.start(60, SECONDS); // Must resolve the resume via the verified skip.
            KReplicaMap<String,String> map2 = m2.getMap();

            assertEquals("v1", map2.get("k1"));
            assertEquals("v2", map2.get("k2"));
            assertEquals("v3", map2.get("k3"));
            assertEquals(3, map2.size());

            // The restarted client must remain fully functional.
            map2.put("k4", "v4");
            assertEquals("v4", awaitDataTopicValue(sharedKafkaTestResource, data, "k4", 30));
        }
    }

    @Test
    void testRestartAfterResumeNotificationDeleted() throws Exception {
        // The flushed ops prefix AND the resume flush notification are deleted, but the unflushed tail
        // is retained: the verified skip must land exactly at the tail start and replay it, so the
        // restarted client sees both the flushed and the unflushed updates.
        String data = "ret_b";
        createTopics(sharedKafkaTestResource, data, data + "_ops", data + "_flush", 1);

        try (KReplicaMapManager m = new KReplicaMapManager(getConfig(data, 300))) {
            m.start(60, SECONDS);
            KReplicaMap<String,String> map = m.getMap();

            map.put("k1", "v1");
            map.put("k2", "v2");
            map.put("k3", "v3");

            for (int i = 1; i <= 3; i++)
                assertEquals("v" + i, awaitDataTopicValue(sharedKafkaTestResource, data, "k" + i, 30));
        }

        // Flushing effectively disabled: these updates stay in the ops topic only.
        try (KReplicaMapManager m2 = new KReplicaMapManager(getConfig(data, NO_LIMIT))) {
            m2.start(60, SECONDS);
            KReplicaMap<String,String> map2 = m2.getMap();

            map2.put("k4", "v4");
            map2.put("k5", "v5");
        }

        TopicPartition opsPart = new TopicPartition(data + "_ops", 0);
        long notification = lastFlushNotificationOffset(data + "_ops");
        deleteOpsRecords(opsPart, notification + 1); // Flushed prefix + notification gone, tail retained.

        try (KReplicaMapManager m3 = new KReplicaMapManager(getConfig(data, 300))) {
            m3.start(60, SECONDS);
            KReplicaMap<String,String> map3 = m3.getMap();

            for (int i = 1; i <= 5; i++)
                assertEquals("v" + i, map3.get("k" + i));

            assertEquals(5, map3.size());
        }
    }

    static void deleteOpsRecords(TopicPartition part, long beforeOffset) throws Exception {
        try (AdminClient admin = sharedKafkaTestResource.getKafkaTestUtils().getAdminClient()) {
            admin.deleteRecords(singletonMap(part, RecordsToDelete.beforeOffset(beforeOffset))).all().get();
        }
    }

    static long endOffset(TopicPartition part) {
        try (KafkaConsumer<byte[],byte[]> consumer = sharedKafkaTestResource.getKafkaTestUtils()
                 .getKafkaConsumer(ByteArrayDeserializer.class, ByteArrayDeserializer.class)) {
            return consumer.endOffsets(singleton(part)).get(part);
        }
    }

    /** The highest ops-topic offset holding a flush notification (the opType is the first serialized byte). */
    static long lastFlushNotificationOffset(String opsTopic) {
        List<ConsumerRecord<byte[],byte[]>> recs =
            sharedKafkaTestResource.getKafkaTestUtils().consumeAllRecordsFromTopic(opsTopic);

        long last = -1;

        for (ConsumerRecord<byte[],byte[]> rec : recs) {
            byte[] value = rec.value();

            if (value != null && value.length > 0 && value[0] == OpMessage.OP_FLUSH_NOTIFICATION)
                last = Math.max(last, rec.offset());
        }

        assertTrue(last >= 0, "No flush notification found in " + opsTopic);
        return last;
    }
}
