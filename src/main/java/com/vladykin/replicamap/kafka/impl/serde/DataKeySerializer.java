package com.vladykin.replicamap.kafka.impl.serde;

import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

/**
 * Null-safe key serializer wrapper for data producer.
 * Explicitly handles null keys (for FlushNotification) without relying on delegate behavior.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class DataKeySerializer<K> implements Serializer<Object> {
    private final Serializer<K> keySerializer;

    public DataKeySerializer(Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        keySerializer.configure(configs, isKey);
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        return keySerializer.serialize(topic, (K) data);
    }

    @Override
    public void close() {
        keySerializer.close();
    }
}