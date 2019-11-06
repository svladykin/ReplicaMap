package com.vladykin.replicamap.kafka.compute;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.kafka.KReplicaMapManagerConfig;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Optimized Compute: {@link Serializer} for the compute functions.
 *
 * @see KReplicaMapManagerConfig#COMPUTE_SERIALIZER_CLASS
 * @see ReplicaMap#compute(Object, BiFunction)
 * @see ReplicaMap#computeIfPresent(Object, BiFunction)
 * @see ReplicaMap#merge(Object, Object, BiFunction)
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public interface ComputeSerializer extends Serializer<BiFunction<?,?,?>> {
    /**
     * Check if we can serialize the given function.
     * If this serializer is unable to serialize the function, then
     * it will not be sent to all the replicas, instead it will be
     * applied locally with the standard algorithm (e.g.
     * {@link ConcurrentMap#compute(Object, BiFunction)} or
     * {@link ConcurrentMap#merge(Object, Object, BiFunction)}).
     *
     * @param function Function.
     * @return {@code true} If this serializer can serialize the given function.
     */
    boolean canSerialize(BiFunction<?,?,?> function);
}
