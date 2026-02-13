package com.vladykin.replicamap.kafka.compute;

import com.vladykin.replicamap.kafka.KReplicaMapManagerConfig;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Optimized Compute: {@link Deserializer} for the compute functions.
 *
 * @see KReplicaMapManagerConfig#COMPUTE_DESERIALIZER_CLASS
 * @see ComputeSerializer
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public interface ComputeDeserializer extends Deserializer<BiFunction<?,?,?>> {
    // Compatibility for older clients.
    @Override
    default void configure(Map<String,?> configs, boolean isKey) {
        // no-op
    }

    @Override
    default void close() {
        // no-op
    }
}
