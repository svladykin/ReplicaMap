package com.vladykin.replicamap.kafka.compute;

import com.vladykin.replicamap.ReplicaMap;
import java.util.function.BiFunction;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * {@link Deserializer} for the compute functions.
 *
 * @see ReplicaMap#compute(Object, BiFunction)
 * @see ReplicaMap#computeIfPresent(Object, BiFunction)
 * @see ReplicaMap#merge(Object, Object, BiFunction)
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public interface BiFunctionDeserializer extends Deserializer<BiFunction<?,?,?>> {
    // no-op
}
