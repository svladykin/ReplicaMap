package com.vladykin.replicamap.kafka;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public interface TestDeserializer<T> extends Deserializer<T> { // For compatibility with older clients.
    @Override
    default void configure(Map<String,?> configs, boolean isKey) {
        // no-op
    }

    @Override
    default void close() {
        // no-op
    }
}
