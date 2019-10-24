package com.vladykin.replicamap.kafka;

import com.vladykin.replicamap.ReplicaNavigableMap;
import java.util.NavigableMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link ReplicaNavigableMap} over Kafka.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class KReplicaNavigableMap<K,V> extends KReplicaMap<K,V> implements ReplicaNavigableMap<K,V> {
    public KReplicaNavigableMap(
        KReplicaMapManager mgr,
        Object id,
        NavigableMap<K,V> map,
        Semaphore opsSemaphore,
        long sendTimeout,
        TimeUnit timeUnit
    ) {
        super(mgr, id, map, opsSemaphore, sendTimeout, timeUnit);
    }

    @Override
    public NavigableMap<K,V> unwrap() {
        return (NavigableMap<K,V>)super.unwrap();
    }
}
