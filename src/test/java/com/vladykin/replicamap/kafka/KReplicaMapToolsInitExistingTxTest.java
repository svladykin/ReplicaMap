package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static com.vladykin.replicamap.kafka.KReplicaMapTools.CMD_INIT_EXISTING;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KReplicaMapToolsInitExistingTxTest {

    static final String DATA = "my_data";
    static final String OPS = "my_ops";
    static final String FLUSH = "my_flush";

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = kafkaClusterWith3Brokers();

    Map<String,Object> getDefaultConfig() {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList(sharedKafkaTestResource.getKafkaConnectString()));

        cfg.put(KReplicaMapManagerConfig.DATA_TOPIC, DATA);
        cfg.put(KReplicaMapManagerConfig.OPS_TOPIC, OPS);
        cfg.put(KReplicaMapManagerConfig.FLUSH_TOPIC, FLUSH);

        return cfg;
    }

    @Test
    void testInitExistingTx() throws Exception {
        KafkaTestUtils u = sharedKafkaTestResource.getKafkaTestUtils();
        createTopics(sharedKafkaTestResource, DATA, OPS, FLUSH, 4);

        Properties prodCfg = new Properties();
        prodCfg.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_tx_id");

        try (Producer<String,String> producer = u.getKafkaProducer(StringSerializer.class, StringSerializer.class, prodCfg)) {
            producer.initTransactions();
            producer.beginTransaction();

            producer.send(new ProducerRecord<>(DATA, 1, "abc", "one"));
            producer.send(new ProducerRecord<>(DATA, 1, "xy", "five"));
            producer.send(new ProducerRecord<>(DATA, 3, "bla", "Bla"));

            producer.commitTransaction();
        }

        try (KReplicaMapManager m = new KReplicaMapManager(getDefaultConfig())) {
            m.start(10, TimeUnit.SECONDS);
            assertTrue(m.getMap().isEmpty());
        }

        KReplicaMapTools.main();
        KReplicaMapTools.main(CMD_INIT_EXISTING, sharedKafkaTestResource.getKafkaConnectString(), DATA, OPS);

        try (KReplicaMapManager m = new KReplicaMapManager(getDefaultConfig())) {
            m.start(10, TimeUnit.SECONDS);
            KReplicaMap<Object,Object> map = m.getMap();
            assertEquals(3, map.size());
            assertEquals("one", map.get("abc"));
            assertEquals("five", map.get("xy"));
            assertEquals("Bla", map.get("bla"));
        }
    }
}