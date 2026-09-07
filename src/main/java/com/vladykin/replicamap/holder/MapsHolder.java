package com.vladykin.replicamap.holder;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.kafka.KReplicaMapManager;
import com.vladykin.replicamap.kafka.KReplicaMapManagerConfig;

import java.util.function.Consumer;

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

    /**
     * Applies the given action to every currently held map. The manager uses it to propagate an
     * unrecoverable failure to the in-flight operations of all the maps, so a custom holder must
     * implement it — otherwise pending operations would silently hang when a worker dies, waiting
     * for an echo that will never arrive.
     *
     * @param action Action to apply to each held map.
     */
    void forEachMap(Consumer<ReplicaMap<?,?>> action);
}
