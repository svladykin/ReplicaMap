package com.vladykin.replicamap.kafka.impl.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntFunction;

/**
 * Thread-safe list allowing to lazily initialize elements and reset them.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class LazyList<T> implements AutoCloseable {
    protected final AtomicReferenceArray<CompletableFuture<T>> arr;

    public LazyList(int size) {
        this.arr = new AtomicReferenceArray<>(size);
    }

    public int size() {
        return arr.length();
    }

    public T get(int index, IntFunction<T> factory) {
        CompletableFuture<T> fut;

        for (;;) {
            fut = arr.get(index);

            if (fut != null)
                break;

            if (factory == null)
                return null;

            if (arr.compareAndSet(index, null, fut = new CompletableFuture<>())) {
                try {
                    fut.complete(factory.apply(index));
                }
                catch (Exception | AssertionError e) {
                    arr.compareAndSet(index, fut, null);
                    fut.completeExceptionally(e);
                }
                break;
            }
        }

        T result = fut.join();

        if (result == null)
            throw new IllegalStateException("List is closed.");

        return result;
    }

    public boolean reset(int index, T exp) {
        Utils.requireNonNull(exp, "exp");

        for (;;) {
            CompletableFuture<T> fut = arr.get(index);

            if (fut == null || !fut.isDone())
                return false;

            assert !fut.isCancelled(); // we never cancel

            if (fut.isCompletedExceptionally())
                return false;

            T actual = fut.join();

            if (exp != actual)
                return false;

            if (arr.compareAndSet(index, fut, null)) {
                Utils.maybeClose(actual);
                return true;
            }
        }
    }

    @Override
    public void close() {
        CompletableFuture<T> closedFut = CompletableFuture.completedFuture(null);

        for (int i = 0; i < arr.length();) {
            T t = null;
            CompletableFuture<T> fut = arr.get(i);

            if (fut != null) {
                try {
                    t = fut.get();
                }
                catch (Exception e) {
                    // ignore
                }
            }

            if (arr.compareAndSet(i, fut, closedFut)) {
                Utils.maybeClose(t);
                i++;
            }
        }
    }
}
