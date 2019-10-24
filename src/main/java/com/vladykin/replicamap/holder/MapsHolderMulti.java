package com.vladykin.replicamap.holder;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.base.ReplicaMapBase;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Convenience holder implementation that contains multiple maps.
 * Needs to define the way to get the map identifier from a key {@link #getMapId(Object)}.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public abstract class MapsHolderMulti extends ConcurrentHashMap<Object,ReplicaMap<?,?>> implements MapsHolder {
    /**
     * Create new inner map to wrap with {@link ReplicaMap}.
     * Override this method to create custom inner maps (possibly depending on map identifier).
     *
     * @param mapId Map id.
     * @return New inner map.
     */
    @SuppressWarnings("unused")
    protected <K,V> Map<K,V> createInnerMap(Object mapId) {
        return new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> ReplicaMap<K,V> getMapById(Object mapId, ReplicaMapFactory<K,V> factory) {
        ReplicaMap<K,V> map = (ReplicaMap<K,V>)get(mapId);

        if (map == null) {
            Map<K,V> innerMap = createInnerMap(mapId);
            map = factory.createReplicaMap(mapId, innerMap);

            ReplicaMap<?,?> old = putIfAbsent(mapId, map);

            if (old != null)
                return (ReplicaMap<K,V>)old;
        }

        return map;
    }

    @Override
    public void close() {
        values().forEach(ReplicaMapBase::interruptRunningOps);
        clear();
    }
}
