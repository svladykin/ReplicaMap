package com.vladykin.replicamap.kafka.impl.worker;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract worker.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public abstract class Worker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    protected final String name;

    protected volatile Thread thread;

    protected final AtomicBoolean started = new AtomicBoolean();
    protected final AtomicBoolean interrupted = new AtomicBoolean();

    public Worker(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    protected Thread newThread() {
        Thread th = new Thread(this, getName());
        th.setDaemon(true);
        th.setUncaughtExceptionHandler((thread, error) -> log.error("Worker died: {}", thread.getName(), error));
        return th;
    }

    public void start() {
        if (started.getAndSet(true))
            throw new IllegalStateException("Worker already was started: " + getName());

        thread = newThread();
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
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Worker failed: {}", getName(), e);
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
        if (interrupted.compareAndSet(false, true)) {
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
        if (interrupted.get())
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
