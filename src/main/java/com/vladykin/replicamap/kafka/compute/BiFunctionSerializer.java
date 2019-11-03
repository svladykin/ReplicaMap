package com.vladykin.replicamap.kafka.compute;

import com.vladykin.replicamap.ReplicaMap;
import java.util.function.BiFunction;
import org.apache.kafka.common.serialization.Serializer;

/**
 * {@link Serializer} for the compute functions.
 *
 * @see ReplicaMap#compute(Object, BiFunction)
 * @see ReplicaMap#computeIfPresent(Object, BiFunction)
 * @see ReplicaMap#merge(Object, Object, BiFunction)
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public interface BiFunctionSerializer extends Serializer<BiFunction<?,?,?>> {
    /**
     * @param function Function.
     * @return {@code true} If this serializer can serialize the given function.
     */
    boolean canSerialize(BiFunction<?,?,?> function);
}
