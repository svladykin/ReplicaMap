package com.vladykin.replicamap.kafka.impl.worker.ops;

import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface OpsUpdateHandler {
    <K,V> boolean applyReceivedUpdate(
        String topic,
        int part,
        long offset,
        UUID clientId,
        long opId,
        byte updateType,
        K key,
        V exp,
        V upd,
        BiFunction<?,?,?> function,
        Consumer<V> updatedValueBox
    );
}
