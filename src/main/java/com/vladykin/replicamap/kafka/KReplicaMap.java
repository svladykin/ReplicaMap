package com.vladykin.replicamap.kafka;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.base.ReplicaMapBase;
import com.vladykin.replicamap.kafka.impl.util.Utils;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Implementation of {@link ReplicaMap} over Kafka.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class KReplicaMap<K,V> extends ReplicaMapBase<K,V> {
    protected final KReplicaMapManager manager;

    public KReplicaMap(
        KReplicaMapManager manager,
        Object id,
        Map<K,V> map,
        Semaphore opsSemaphore,
        boolean checkPrecondition,
        Duration sendTimeout
    ) {
        super(id, map, opsSemaphore, checkPrecondition, sendTimeout);
        this.manager = Utils.requireNonNull(manager, "mgr");
    }

    @Override
    public KReplicaMapManager getManager() {
        return manager;
    }

    @Override
    public Map<K,V> unwrap() {
        manager.checkRunning();
        return super.unwrap();
    }

    @Override
    protected void beforeStart(AsyncOp<?,K,V> op) {
        manager.checkRunning();
    }

    @Override
    protected void sendUpdate(
        long opId,
        byte updateType,
        K key,
        V exp,
        V upd,
        BiFunction<?,?,?> function,
        Consumer<Throwable> onSendFailed
    ) {
        manager.sendUpdate(this, opId, updateType, key, exp, upd, function, onSendFailed);
    }

    @Override
    protected boolean canSendFunction(BiFunction<?,?,?> function) {
        return manager.canSendFunction(function);
    }
}
