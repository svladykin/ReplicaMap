package com.vladykin.replicamap.holder;

import com.vladykin.replicamap.ReplicaMap;

import java.util.Map;

@FunctionalInterface
public interface ReplicaMapFactory<K,V> {
    ReplicaMap<K,V> createReplicaMap(Object mapId, Map<K,V> map);
}
