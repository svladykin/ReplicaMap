package com.vladykin.replicamap.kafka.impl.worker.ops;

import com.vladykin.replicamap.kafka.impl.util.Box;
import java.util.function.BiFunction;

public interface OpsUpdateHandler {
    <K,V> boolean applyReceivedUpdate(
        String topic,
        int part,
        long offset,
        long clientId,
        long opId,
        byte updateType,
        K key,
        V exp,
        V upd,
        BiFunction<?,?,?> function,
        Box<V> updatedValueBox
    );
}
