package com.vladykin.replicamap;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public class TestMap<K,V> implements ReplicaMap<K,V> {
    private Object id;
    public Map<K,V> m;
    protected final AtomicReference<ReplicaMapListener<K,V>> listener = new AtomicReference<>();

    public TestMap(Map<K,V> m) {
        this(0, m);
    }

    public TestMap(Object id, Map<K,V> m) {
        this.id = id;
        this.m = m;
    }

    @Override
    public Object id() {
        return id;
    }

    @Override
    public Map<K,V> unwrap() {
        return m;
    }

    @Override
    public CompletableFuture<V> asyncPut(K key, V value) {
        CompletableFuture<V> fut = new CompletableFuture<>();
        try {
            fut.complete(unwrap().put(key, value));
        }
        catch (RuntimeException e) {
            fut.completeExceptionally(e);
        }
        return fut;
    }

    @Override
    public CompletableFuture<V> asyncPutIfAbsent(K key, V value) {
        CompletableFuture<V> fut = new CompletableFuture<>();
        try {
            fut.complete(unwrap().putIfAbsent(key, value));
        }
        catch (RuntimeException e) {
            fut.completeExceptionally(e);
        }
        return fut;
    }

    @Override
    public CompletableFuture<V> asyncReplace(K key, V value) {
        CompletableFuture<V> fut = new CompletableFuture<>();
        try {
            fut.complete(unwrap().replace(key, value));
        }
        catch (RuntimeException e) {
            fut.completeExceptionally(e);
        }
        return fut;
    }

    @Override
    public CompletableFuture<Boolean> asyncReplace(K key, V oldValue, V newValue) {
        CompletableFuture<Boolean> fut = new CompletableFuture<>();
        try {
            fut.complete(unwrap().replace(key, oldValue, newValue));
        }
        catch (RuntimeException e) {
            fut.completeExceptionally(e);
        }
        return fut;
    }

    @Override
    public CompletableFuture<V> asyncRemove(K key) {
        CompletableFuture<V> fut = new CompletableFuture<>();
        try {
            fut.complete(unwrap().remove(key));
        }
        catch (RuntimeException e) {
            fut.completeExceptionally(e);
        }
        return fut;
    }

    @Override
    public CompletableFuture<Boolean> asyncRemove(K key, V value) {
        CompletableFuture<Boolean> fut = new CompletableFuture<>();
        try {
            fut.complete(unwrap().remove(key, value));
        }
        catch (RuntimeException e) {
            fut.completeExceptionally(e);
        }
        return fut;
    }

    @Override
    public CompletableFuture<V> asyncCompute(K key, BiFunction<? super K,? super V,? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<V> asyncComputeIfPresent(K key,
        BiFunction<? super K,? super V,? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<V> asyncMerge(K key, V value,
        BiFunction<? super V,? super V,? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setListener(ReplicaMapListener<K,V> listener) {
        this.listener.set(listener);
    }

    @Override
    public ReplicaMapListener<K,V> getListener() {
        return listener.get();
    }

    @Override
    public boolean casListener(ReplicaMapListener<K,V> expected, ReplicaMapListener<K,V> newListener) {
        return listener.compareAndSet(expected, newListener);
    }
}
