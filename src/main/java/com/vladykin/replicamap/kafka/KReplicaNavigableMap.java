package com.vladykin.replicamap.kafka;

import com.vladykin.replicamap.ReplicaNavigableMap;

import java.time.Duration;
import java.util.NavigableMap;
import java.util.concurrent.Semaphore;

/**
 * Implementation of {@link ReplicaNavigableMap} over Kafka.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class KReplicaNavigableMap<K,V> extends KReplicaMap<K,V> implements ReplicaNavigableMap<K,V> {
    public KReplicaNavigableMap(
        KReplicaMapManager mgr,
        Object id,
        NavigableMap<K,V> map,
        Semaphore opsSemaphore,
        boolean checkPrecondition,
        Duration sendTimeout
    ) {
        super(mgr, id, map, opsSemaphore, checkPrecondition, sendTimeout);
    }

    @Override
    public NavigableMap<K,V> unwrap() {
        return (NavigableMap<K,V>)super.unwrap();
    }
}
