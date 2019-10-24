package com.vladykin.replicamap.kafka.impl.worker;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract worker.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public abstract class Worker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    protected static final AtomicReferenceFieldUpdater<Worker, Thread> THREAD =
        AtomicReferenceFieldUpdater.newUpdater(Worker.class, Thread.class, "thread");
    protected static final AtomicIntegerFieldUpdater<Worker> INTERRUPTED =
        AtomicIntegerFieldUpdater.newUpdater(Worker.class, "interrupted");

    protected final int workerId;
    protected final String group;

    protected volatile Thread thread;
    protected volatile int interrupted;

    public Worker(String group, int workerId) {
        this.group = group;
        this.workerId = workerId;
    }

    public String getName() {
        return group + "-" + workerId;
    }

    protected Thread newThread() {
        Thread th = new Thread(this, getName());
        th.setDaemon(true);
        th.setUncaughtExceptionHandler((thread, error) -> log.error("Worker died: " + thread.getName(), error));
        return th;
    }

    public void start() {
        if (!THREAD.compareAndSet(this, null, newThread()))
            throw new IllegalStateException("Worker already was started: " + getName());

        thread.start();
    }

    @Override
    public String toString() {
        return "Worker[" + getName() + "]";
    }

    @Override
    public void run() {
        log.debug("Worker started: {}", getName());

        try {
            checkInterrupted();
            doRun();
        }
        catch (Exception | AssertionError e) {
            if (!Utils.isInterrupted(e))
                log.error("Worker failed: " + getName(), e);
        }
        finally {
            log.debug("Worker stopped: {}", getName());
        }
    }

    protected abstract void doRun() throws Exception;

    protected void checkInterrupted() {
        if (isInterrupted())
            throw new ReplicaMapException("Interrupted.", new InterruptedException());
    }

    public boolean interrupt() {
        if (INTERRUPTED.compareAndSet(this, 0, 1)) {
            interruptThread();

            return true;
        }
        return false;
    }

    protected void interruptThread() {
        Thread thread = this.thread;
        if (thread != null)
            thread.interrupt();
    }

    public boolean isInterrupted() {
        if (interrupted != 0)
            return true;

        Thread th = thread;
        return th != null && th.isInterrupted();
    }

    public static void interruptAll(Iterable<? extends Worker> workers) {
        if (workers == null)
            return;

        for (Worker worker : workers) {
            if (worker != null)
                worker.interrupt();
        }
    }

    public static void joinAll(Iterable<? extends Worker> workers) {
        if (workers == null)
            return;

        for (Worker worker : workers) {
            if (worker == null)
                continue;

            Thread th = worker.thread;
            if (th != null) {
                try {
                    th.join();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
}
