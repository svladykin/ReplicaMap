package com.vladykin.replicamap;

/**
 * Listener for {@link ReplicaMap} updates.
 * Fires only for the actual successful updates, not for failed update attempts.
 * Slow operations should not be executed in listener since they will block further map updates.
 *
 * @see ReplicaMap#getListener()
 * @see ReplicaMap#setListener(ReplicaMapListener)
 * @see ReplicaMap#casListener(ReplicaMapListener, ReplicaMapListener)
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public interface ReplicaMapListener<K,V> {
    /**
     * Called after every successful map update.
     *
     * @param map Updated map instance (useful when a single listener is installed to
     *            many {@link ReplicaMap} instances).
     * @param myUpdate {@code true} If the update was issued by the given map instance,
     *                 {@code false} if the update was replicated from another instance.
     * @param key Key.
     * @param oldValue Old value or {@code null} if absent before the put.
     * @param newValue New value or {@code null} if removed.
     * @throws Exception If failed.
     */
    void onMapUpdate(ReplicaMap<K,V> map, boolean myUpdate, K key, V oldValue, V newValue) throws Exception;
}
