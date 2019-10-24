package com.vladykin.replicamap;

import com.vladykin.replicamap.kafka.KReplicaMapManager;
import com.vladykin.replicamap.kafka.KReplicaMapManagerConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.Test;

public class FlowersTest {
    static final long DELETE_DELAY_MS = 7200000;
    static final String BOOTSTRAP_SERVER = "localhost:9092";

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

        flowers.putIfAbsent(1L, "{name: 'rose', price: 100}");
        flowers.putIfAbsent(2L, "{name: 'magnolia', price: 120}");
        flowers.putIfAbsent(3L, "{name: 'cactus', price: 150}");

        System.out.println(new TreeMap<>(flowers));
    }
}
