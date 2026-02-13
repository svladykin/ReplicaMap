package com.vladykin.replicamap.kafka.impl.serde;

import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

/**
 * Value serializer for data producer that handles both map values and OpMessage types.
 * Used by dataProducer to send both data records and FlushNotification in same transaction.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class DataValueSerializer<V> implements Serializer<Object> {
    private final Serializer<V> valueSerializer;
    private final OpMessageSerializer<V> opMessageSerializer;

    public DataValueSerializer(Serializer<V> valueSerializer, OpMessageSerializer<V> opMessageSerializer) {
        this.valueSerializer = valueSerializer;
        this.opMessageSerializer = opMessageSerializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        valueSerializer.configure(configs, isKey);
        opMessageSerializer.configure(configs, isKey);
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof OpMessage) {
            return opMessageSerializer.serialize(topic, (OpMessage) data);
        }
        return valueSerializer.serialize(topic, (V) data);
    }

    @Override
    public void close() {
        valueSerializer.close();
        opMessageSerializer.close();
    }
}