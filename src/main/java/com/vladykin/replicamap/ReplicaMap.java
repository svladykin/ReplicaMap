package com.vladykin.replicamap;

import com.vladykin.replicamap.holder.MapsHolder;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Replicated {@link ConcurrentMap} with async operations.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public interface ReplicaMap<K,V> extends ConcurrentMap<K,V> {
    /**
     * Gets the identifier of this map.
     *
     * @return Map id.
     * @see ReplicaMapManager#getMap(Object)
     * @see MapsHolder#getMapId(Object)
     */
    Object id();

    /**
     * Gets the manager for this map.
     *
     * @return Map manager.
     * @see ReplicaMapManager#getMap(Object)
     * @see ReplicaMapManager#getMap()
     */
    ReplicaMapManager getManager();

    /**
     * Gets the underlying map that actually stores the data.
     *
     * The returned map must not be updated directly,
     * only through {@link ReplicaMap} methods.
     * Use this method at your own risk.
     *
     * @return The underlying map.
     */
    Map<K, V> unwrap();

    /**
     * Asynchronous version of {@link Map#put(Object, Object)}.
     *
     * @param key Key.
     * @param value Value.
     * @return Future.
     */
    CompletableFuture<V> asyncPut(K key, V value);

    /**
     * Asynchronous version of {@link Map#putIfAbsent(Object, Object)}.
     *
     * @param key Key.
     * @param value Value.
     * @return Future.
     */
    CompletableFuture<V> asyncPutIfAbsent(K key, V value);

    /**
     * Asynchronous version of {@link Map#replace(Object, Object)}.
     *
     * @param key Key.
     * @param value Value.
     * @return Future.
     */
    CompletableFuture<V> asyncReplace(K key, V value);

    /**
     * Asynchronous version of {@link Map#replace(Object, Object, Object)}.
     *
     * @param key Key.
     * @param oldValue Expected value.
     * @param newValue New value.
     * @return Future.
     */
    CompletableFuture<Boolean> asyncReplace(K key, V oldValue, V newValue);

    /**
     * Asynchronous version of {@link Map#remove(Object)}.
     *
     * @param key Key.
     * @return Future.
     */
    CompletableFuture<V> asyncRemove(K key);

    /**
     * Asynchronous version of {@link Map#remove(Object, Object)}.
     *
     * @param key Key.
     * @param value Expected value.
     * @return Future.
     */
    CompletableFuture<Boolean> asyncRemove(K key, V value);

    /**
     * Asynchronous version of {@link Map#compute(Object, BiFunction)}.
     *
     * @param key Key.
     * @param remappingFunction Function to compute a value.
     * @return Future.
     */
    CompletableFuture<V> asyncCompute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

    /**
     * Asynchronous version of {@link Map#computeIfAbsent(Object, Function)}.
     *
     * @param key Key.
     * @param mappingFunction Function to compute a value.
     * @return Future.
     */
    default CompletableFuture<V> asyncComputeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        Utils.requireNonNull(mappingFunction, "mappingFunction");

        V oldValue = get(key);
        if (oldValue != null)
            return CompletableFuture.completedFuture(oldValue);

        V newValue = mappingFunction.apply(key);
        return asyncPutIfAbsent(key, newValue).thenApply(v -> v == null ? newValue : v);
    }

    /**
     * Asynchronous version of {@link Map#computeIfPresent(Object, BiFunction)}.
     *
     * @param key Key.
     * @param remappingFunction Function to compute a value.
     * @return Future.
     */
    CompletableFuture<V> asyncComputeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

    /**
     * Asynchronous version of {@link Map#merge(Object, Object, BiFunction)}.
     *
     * @param key Key.
     * @param value Non-null value to be merged with the existing value
     *              associated with the key or, if no existing value or a null value
     *              is associated with the key, to be associated with the key
     * @param remappingFunction function to recompute a value if present.
     * @return Future.
     */
    CompletableFuture<V> asyncMerge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction);

    /**
     * Sets the listener for the map updates.
     *
     * @param listener Listener.
     */
    void setListener(ReplicaMapListener<K,V> listener);

    /**
     * Gets the listener for the map updates.
     *
     * @return Listener or {@code null} if none.
     */
    ReplicaMapListener<K,V> getListener();

    /**
     * Atomically sets the new listener if the expected one is still there.
     *
     * @param expected Expected current listener.
     * @param newListener New listener to set.
     * @return {@code true} If successful.
     */
    boolean casListener(ReplicaMapListener<K,V> expected, ReplicaMapListener<K,V> newListener);

    @Override
    default V putIfAbsent(K key, V value) {
        try {
            return asyncPutIfAbsent(key, value).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new ReplicaMapException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    default boolean remove(Object key, Object value) {
        try {
            return asyncRemove((K)key, (V)value).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new ReplicaMapException(e);
        }
    }

    @Override
    default boolean replace(K key, V oldValue, V newValue) {
        try {
            return asyncReplace(key, oldValue, newValue).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new ReplicaMapException(e);
        }
    }

    @Override
    default V replace(K key, V value) {
        try {
            return asyncReplace(key, value).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new ReplicaMapException(e);
        }
    }

    @Override
    default V put(K key, V value) {
        try {
            return asyncPut(key, value).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new ReplicaMapException(e);
        }
    }

    @Override
    default void putAll(Map<? extends K,? extends V> m) {
        Utils.requireNonNull(m, "m");

        List<CompletableFuture<V>> futures = new ArrayList<>();

        for (Map.Entry<? extends K, ? extends V> e : m.entrySet())
            futures.add(asyncPut(e.getKey(), e.getValue()));

        try {
            Utils.allOf(futures).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new ReplicaMapException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    default V remove(Object key) {
        Utils.requireNonNull(key, "key");
        try {
            return asyncRemove((K)key).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new ReplicaMapException(e);
        }
    }

    @Override
    default void clear() {
        List<CompletableFuture<V>> futures = new ArrayList<>();

        for (K k : unwrap().keySet())
            futures.add(asyncRemove(k));

        try {
            Utils.allOf(futures).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new ReplicaMapException(e);
        }
    }

    @Override
    default int size() {
        return unwrap().size();
    }

    @Override
    default boolean isEmpty() {
        return unwrap().isEmpty();
    }

    @Override
    default boolean containsKey(Object key) {
        Utils.requireNonNull(key, "key");
        return unwrap().containsKey(key);
    }

    @Override
    default boolean containsValue(Object value) {
        Utils.requireNonNull(value, "value");
        return unwrap().containsValue(value);
    }

    @Override
    default V get(Object key) {
        Utils.requireNonNull(key, "key");
        return unwrap().get(key);
    }

    @Override
    default Set<K> keySet() {
        return Collections.unmodifiableSet(unwrap().keySet());
    }

    @Override
    default Collection<V> values() {
        return Collections.unmodifiableCollection(unwrap().values());
    }

    @Override
    default Set<Entry<K,V>> entrySet() {
        return Collections.unmodifiableSet(unwrap().entrySet());
    }
}
