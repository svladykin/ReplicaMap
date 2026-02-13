package com.vladykin.replicamap.kafka.impl.util;

import java.util.concurrent.Semaphore;

@SuppressWarnings({"AssertWithSideEffects"})
public class NonReentrantLock implements AutoCloseable {

    protected final Semaphore semaphore = new Semaphore(1);
    protected volatile Thread owner;

    public void lock() {
        semaphore.acquireUninterruptibly();
        assert assertAfterLock();
    }

    public boolean tryLock() {
        if (semaphore.tryAcquire()) {
            assert assertAfterLock();
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        assert assertBeforeUnlock();
        semaphore.release();
    }

    protected boolean assertAfterLock() {
        int permits = semaphore.availablePermits();
        if (permits != 0)
            throw new IllegalStateException("availablePermits: " + permits);

        if (owner != null)
            throw new IllegalStateException("owner: " + owner);

        owner = Thread.currentThread();
        return true;
    }

    protected boolean assertBeforeUnlock() {
        int permits = semaphore.availablePermits();
        if (permits != 0)
            throw new IllegalStateException("availablePermits: " + permits);

        if (owner != Thread.currentThread())
            throw new IllegalStateException("owner: " + owner);

        owner = null;
        return true;
    }
}
