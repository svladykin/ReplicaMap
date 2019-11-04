package com.vladykin.replicamap.base;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

@SuppressWarnings("WeakerAccess")
public abstract class TestReplicaMapBase<K, V> extends ReplicaMapBase<K, V> {
    private BiConsumer<TestReplicaMapUpdate<K, V>, FailureCallback> queue;

    TestReplicaMapBase(
        char id,
        Map<K,V> map,
        Semaphore maxActiveOps
    ) {
        this(id, map, maxActiveOps, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    public TestReplicaMapBase(
        char id,
        Map<K,V> map,
        Semaphore maxActiveOps,
        long sendTimeout,
        TimeUnit timeUnit
    ) {
        super(id, map, maxActiveOps, sendTimeout, timeUnit);
    }

    @Override
    protected void sendUpdate(long opId, byte updateType, K key, V exp, V upd, BiFunction<?,?,?> function, FailureCallback callback) {
        doSendUpdate(new TestReplicaMapUpdate<>(opId, updateType, key, exp, upd, function, id), callback);
    }

    protected abstract void doSendUpdate(TestReplicaMapUpdate<K, V> update, FailureCallback callback);

    public void update(boolean myUpdate, TestReplicaMapUpdate<K, V> u) {
        onReceiveUpdate(myUpdate, u.opId, u.updateType, u.key, u.exp, u.upd, u.function, null);
    }

    @Override
    protected boolean canSendFunction(BiFunction<?,?,?> function) {
        return false; // FIXME
    }
}
