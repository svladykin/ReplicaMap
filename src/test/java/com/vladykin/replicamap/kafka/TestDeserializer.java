package com.vladykin.replicamap.kafka;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

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
