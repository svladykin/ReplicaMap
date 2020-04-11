package com.vladykin.replicamap.base;

import com.vladykin.replicamap.ReplicaMapManager;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

@SuppressWarnings("WeakerAccess")
public abstract class TestReplicaMapBase<K, V> extends ReplicaMapBase<K, V> {
    TestReplicaMapBase(
        char id,
        Map<K,V> map,
        Semaphore maxActiveOps
    ) {
        this(id, map, maxActiveOps, true, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    public TestReplicaMapBase(
        char id,
        Map<K,V> map,
        Semaphore maxActiveOps,
        boolean checkPrecondition,
        long sendTimeout,
        TimeUnit timeUnit
    ) {
        super(id, map, maxActiveOps, checkPrecondition, sendTimeout, timeUnit);
    }

    @Override
    protected void sendUpdate(long opId, byte updateType, K key, V exp, V upd, BiFunction<?,?,?> function, Consumer<Throwable> callback) {
        doSendUpdate(new TestReplicaMapUpdate<>(opId, updateType, key, exp, upd, function, id), callback);
    }

    protected abstract void doSendUpdate(TestReplicaMapUpdate<K, V> update, Consumer<Throwable> callback);

    public void update(boolean myUpdate, TestReplicaMapUpdate<K, V> u) {
        onReceiveUpdate(myUpdate, u.opId, u.updateType, u.key, u.exp, u.upd, u.function, null);
    }

    @Override
    protected boolean canSendFunction(BiFunction<?,?,?> function) {
        return false;
    }

    @Override
    public ReplicaMapManager getManager() {
        throw new UnsupportedOperationException();
    }
}
