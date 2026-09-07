package com.vladykin.replicamap;

import com.vladykin.replicamap.holder.MapsHolder;
import com.vladykin.replicamap.kafka.impl.util.Utils;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
     * @param timeout Timeout, must be positive.
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
     * @param timeout Timeout, must be positive.
     */
    default void start(Duration timeout) {
        start(Utils.toNanosSaturated(timeout), TimeUnit.NANOSECONDS);
    }

    /**
     * Asynchronously stop the {@link ReplicaMap} manager and release all the resources.
     *
     * <p>Never blocks, thus it is safe to call from any thread, including a {@link ReplicaMap}
     * update callback (a listener or a replicated compute/merge function) that runs on a worker
     * thread the stop itself has to join. Idempotent: repeated and concurrent calls all return
     * the same future.
     *
     * @return Future that completes when the manager is stopped and all the resources are released.
     */
    CompletableFuture<ReplicaMapManager> stop();

    /**
     * Stop the manager and wait until it will be stopped or for the specified timeout.
     * On timeout the stop still proceeds in the background and the future returned by
     * {@link #stop()} completes later.
     *
     * @param timeout Timeout, a negative one waits indefinitely.
     * @param unit Time unit.
     */
    default void stop(long timeout, TimeUnit unit) {
        Utils.requireNonNull(unit, "unit");

        try {
            if (timeout < 0)
                stop().get();
            else
                stop().get(timeout, unit);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ReplicaMapException("Failed to stop manager.", e);
        }
        catch (ExecutionException e) {
            throw new ReplicaMapException("Failed to stop manager.", e.getCause());
        }
        catch (TimeoutException e) {
            throw new ReplicaMapException("Timed out waiting for the manager to stop, " +
                "the stop still proceeds in the background.", e);
        }
    }

    /**
     * Stop the manager and wait until it will be stopped or for the specified timeout.
     *
     * @param timeout Timeout, a negative one waits indefinitely.
     */
    default void stop(Duration timeout) {
        stop(Utils.toNanosSaturated(timeout), TimeUnit.NANOSECONDS);
    }

    /**
     * Stop the manager and wait for the stop to complete.
     *
     * @throws ReplicaMapException If the stop failed, or did not complete within the implementation's
     *         timeout — in which case the stop still proceeds in the background and the future
     *         returned by {@link #stop()} completes when it is done.
     */
    @Override
    void close();

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
