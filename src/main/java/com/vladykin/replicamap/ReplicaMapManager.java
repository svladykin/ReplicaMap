package com.vladykin.replicamap;

import com.vladykin.replicamap.holder.MapsHolder;
import com.vladykin.replicamap.kafka.impl.util.Utils;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * The component that manages replicated maps lifecycle.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public interface ReplicaMapManager extends AutoCloseable {
    /**
     * Asynchronously start the manager.
     *
     * @return Future that completes when all the needed operations are done and the manager is operational.
     */
    CompletableFuture<ReplicaMapManager> start();

    /**
     * Synchronously start the manager and wait until it will be started or for the specified timeout.
     *
     * @param timeout Timeout.
     * @param unit Time unit.
     */
    default void start(long timeout, TimeUnit unit) {
        Utils.checkPositive(timeout, "timeout");
        Utils.requireNonNull(unit, "unit");

        try {
            start().get(timeout, unit);
        }
        catch (Exception e) {
            throw new ReplicaMapException("Failed to start manager.", e);
        }
    }

    /**
     * Synchronously start the manager and wait until it will be started or for the specified timeout.
     *
     * @param timeout Timeout.
     */
    default void start(Duration timeout) {
        start(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Stop the {@link ReplicaMap} manager and release all the resources.
     */
    void stop();

    /**
     * The same as {@link #stop}.
     */
    @Override
    default void close() {
        stop();
    }

    /**
     * Get the map instance by the given identifier.
     *
     * @param mapId Map identifier.
     * @return Map.
     * @see MapsHolder#getMapId(Object)
     */
    <K,V,M extends ReplicaMap<K,V>> M getMap(Object mapId);

    /**
     * Get the default map instance.
     *
     * @return Default map.
     * @see MapsHolder#getDefaultMapId()
     * @throws UnsupportedOperationException If there is no default map.
     */
    <K,V,M extends ReplicaMap<K,V>> M getMap();
}
