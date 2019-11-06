package com.vladykin.replicamap.kafka.compute;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.kafka.KReplicaMapManagerConfig;
import java.util.function.BiFunction;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Optimized Compute: {@link Deserializer} for the compute functions.
 *
 * @see KReplicaMapManagerConfig#COMPUTE_DESERIALIZER_CLASS
 * @see ReplicaMap#compute(Object, BiFunction)
 * @see ReplicaMap#computeIfPresent(Object, BiFunction)
 * @see ReplicaMap#merge(Object, Object, BiFunction)
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public interface ComputeDeserializer extends Deserializer<BiFunction<?,?,?>> {
    // no-op
}
