package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMapException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.createTopics;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleTest.kafkaClusterWith3Brokers;
import static com.vladykin.replicamap.kafka.KReplicaMapTools.CMD_INIT_EXISTING;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KReplicaMapToolsInitExistingTest {

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
    void testInitExisting() throws Exception {
        KafkaTestUtils u = sharedKafkaTestResource.getKafkaTestUtils();
        createTopics(sharedKafkaTestResource, DATA, OPS, FLUSH, 4);

        Map<byte[],byte[]> recs = new HashMap<>();
        recs.put("abc".getBytes(UTF_8), "one".getBytes(UTF_8));
        recs.put("xy".getBytes(UTF_8), "five".getBytes(UTF_8));
        u.produceRecords(recs, DATA, 1);

        recs = new HashMap<>();
        recs.put("bla".getBytes(UTF_8), "Bla".getBytes(UTF_8));
        u.produceRecords(recs, DATA, 3);

        assertThrows(ReplicaMapException.class, () -> {
            try (KReplicaMapManager m = new KReplicaMapManager(getDefaultConfig())) {
                m.start(10, TimeUnit.SECONDS);
            }
        });

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