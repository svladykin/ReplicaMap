package com.vladykin.replicamap.base;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TestMultiQueue<K,V> implements BiConsumer<TestReplicaMapUpdate<K,V>, Consumer<Throwable>> {
    private final ReentrantLock lock = new ReentrantLock();
    private final List<ConcurrentLinkedQueue<TestReplicaMapUpdate<K,V>>> queues = new ArrayList<>();

    @Override
    public void accept(TestReplicaMapUpdate<K,V> update, Consumer<Throwable> callback) {
        lock.lock();
        try {
            for (ConcurrentLinkedQueue<TestReplicaMapUpdate<K,V>> queue : queues)
                queue.add(update);
        }
        finally {
            lock.unlock();
        }
    }

    Supplier<TestReplicaMapUpdate<K,V>> newSupplier() {
        ConcurrentLinkedQueue<TestReplicaMapUpdate<K,V>> q = new ConcurrentLinkedQueue<>();
        lock.lock();
        try {
            queues.add(q);
        }
        finally {
            lock.unlock();
        }
        return q::poll;
    }

    @SuppressWarnings("BusyWait")
    void awaitEmpty() throws InterruptedException {
        outerLoop: for (;;) {
            lock.lock();
            try {
                for (ConcurrentLinkedQueue<TestReplicaMapUpdate<K,V>> queue : queues) {
                    if (!queue.isEmpty()) {
                        Thread.sleep(3);
                        continue outerLoop;
                    }
                }
            }
            finally {
                lock.unlock();
            }
            break;
        }
    }
}
