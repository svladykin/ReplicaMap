package com.vladykin.replicamap.kafka;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public interface TestSerializer<T> extends Serializer<T> { // For compatibility with older clients.
    @Override
    default void configure(Map<String,?> configs, boolean isKey) {
        // no-op
    }

    @Override
    default void close() {
        // no-op
    }
}
