package com.vladykin.replicamap.holder;

import com.vladykin.replicamap.ReplicaMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.vladykin.replicamap.base.ReplicaMapBase.interruptRunningOps;

/**
 * Convenience holder implementation that contains only a single map.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class MapsHolderSingle extends AtomicReference<ReplicaMap<?,?>> implements MapsHolder {
    /**
     * Create new inner map to wrap with {@link ReplicaMap}.
     * Override this method to create custom inner map.
     *
     * @return New inner map.
     */
    protected <K,V> Map<K,V> createInnerMap() {
        return new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K,V> ReplicaMap<K,V> getMapById(Object mapId, ReplicaMapFactory<K,V> factory) {
        if (mapId != getDefaultMapId())
            throw new IllegalArgumentException("Unexpected map id: " + mapId);

        ReplicaMap<K,V> map = (ReplicaMap<K,V>)get();

        if (map == null) {
            Map<K,V> innerMap = createInnerMap();
            map = factory.createReplicaMap(mapId, innerMap);

            if (!compareAndSet(null, map))
                return (ReplicaMap<K,V>)get();
        }

        return map;
    }

    @Override
    public Object getDefaultMapId() {
        return null;
    }

    @Override
    public <K> Object getMapId(K key) {
        return getDefaultMapId();
    }

    @Override
    public void close() {
        interruptRunningOps(getAndSet(null));
    }
}
