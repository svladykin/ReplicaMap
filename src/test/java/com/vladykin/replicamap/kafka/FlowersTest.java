package com.vladykin.replicamap.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.ReplicaMapManager;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class FlowersTest {
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

    static String BOOTSTRAP_SERVER = "localhost:9092";
    static final long DELETE_DELAY_MS = 7200000;

    @BeforeEach
    void beforeTest() {
        BOOTSTRAP_SERVER = sharedKafkaTestResource.getKafkaConnectString();
    }

    @Test
    void test() {
        // Setup the configuration like in Kafka (see all the options in KReplicaMapManagerConfig).
        Map<String,Object> cfg = new HashMap<>();
        cfg.put(KReplicaMapManagerConfig.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVER);
        cfg.put(KReplicaMapManagerConfig.DATA_TOPIC, "flowers");
        cfg.put(KReplicaMapManagerConfig.OPS_TOPIC, "flowers_ops");
        cfg.put(KReplicaMapManagerConfig.FLUSH_TOPIC, "flowers_flush");

        // Setup the serialization for map keys and values, by default String serialization is used.
        cfg.put(KReplicaMapManagerConfig.KEY_SERIALIZER_CLASS, LongSerializer.class);
        cfg.put(KReplicaMapManagerConfig.KEY_DESERIALIZER_CLASS, LongDeserializer.class);

        // Create and start the KReplicaMapManager instance.
        ReplicaMapManager replicaMapManager = new KReplicaMapManager(cfg);

        // Here we use DELETE_DELAY_MS that we have used for the topics configuration
        // and adjusted it by 3 minutes to mitigate the possible clock skew.
        replicaMapManager.start(DELETE_DELAY_MS - 180_000, TimeUnit.MILLISECONDS);

        // Get the ReplicaMap instance and use it as if it was a typical ConcurrentMap,
        // but all the updates will be visible to all the other clients even after restarts.
        ReplicaMap<Long, String> flowers = replicaMapManager.getMap();

        flowers.putIfAbsent(1L, "Rose");
        flowers.putIfAbsent(2L, "Magnolia");
        flowers.putIfAbsent(3L, "Cactus");

        System.out.println(new TreeMap<>(flowers));
    }
}
