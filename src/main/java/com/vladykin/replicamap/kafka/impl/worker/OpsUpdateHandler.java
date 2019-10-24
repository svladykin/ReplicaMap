package com.vladykin.replicamap.kafka.impl.worker;

public interface OpsUpdateHandler {
    <K,V> boolean applyReceivedUpdate(long clientId, long opId, byte updateType, K key, V exp, V upd);
}
