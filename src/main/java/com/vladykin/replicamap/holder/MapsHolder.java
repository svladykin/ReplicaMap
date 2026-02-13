package com.vladykin.replicamap.holder;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.kafka.KReplicaMapManager;
import com.vladykin.replicamap.kafka.KReplicaMapManagerConfig;

/**
 * Holds the maps for {@link ReplicaMapManager}.
 * For {@link KReplicaMapManager} can be set by {@link KReplicaMapManagerConfig#MAPS_HOLDER} configuration property.
 *
 * @see MapsHolderSingle
 * @see MapsHolderMulti
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public interface MapsHolder extends AutoCloseable {
    /**
     * Gets existing or creates a new map for the given id.
     *
     * @param mapId Map id.
     * @param factory Replica map factory.
     * @return Map.
     */
    <K,V> ReplicaMap<K,V> getMapById(Object mapId, ReplicaMapFactory<K,V> factory);

    /**
     * Extracts map id from the given key.
     * The returned object must correctly implement {@link Object#hashCode()} and {@link Object#equals(Object)} methods.
     *
     * @param key Key.
     * @return Map id for the key.
     * @see ReplicaMapManager#getMap(Object)
     */
    <K> Object getMapId(K key);

    /**
     * Gets id of the default replica map or throws an exception if not supported.
     *
     * @return Default map id.
     * @see ReplicaMapManager#getMap()
     */
    default Object getDefaultMapId() {
        throw new UnsupportedOperationException();
    }
}
