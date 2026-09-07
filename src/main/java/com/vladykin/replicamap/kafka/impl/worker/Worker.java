package com.vladykin.replicamap.kafka.impl.worker;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.KReplicaMapManager;
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

    public final String name;
    public final KReplicaMapManager manager;

    protected volatile WorkerThread<?> thread;

    protected final AtomicBoolean started = new AtomicBoolean();
    protected final AtomicBoolean interrupted = new AtomicBoolean();

    public Worker(String name, KReplicaMapManager manager) {
        this.name = name;
        this.manager = manager;
    }

    protected WorkerThread<?> newThread() {
        var th = new DefaultWorkerThread(this);
        th.setDaemon(true);
        th.setUncaughtExceptionHandler((thread, error) -> log.error("Worker died: {}", thread.getName(), error));
        return th;
    }

    public void start() {
        if (started.getAndSet(true))
            throw new IllegalStateException("Worker already was started: " + name);

        thread = newThread();
        thread.start();
    }

    @Override
    public String toString() {
        return "Worker[" + name + "]";
    }

    @Override
    public void run() {
        log.debug("Worker started: {}", name);

        try {
            checkInterrupted();
            doRun();
        }
        catch (Throwable e) {
            if (!Utils.isInterrupted(e)) {
                log.error("Worker failed: {}", name, e);

                // Record the failure on the manager so subsequent map operations and the other
                // workers fail fast, instead of this worker's duties silently going stale.
                if (manager != null)
                    manager.onUnrecoverableError(e);
            }
        }
        finally {
            log.debug("Worker stopped: {}", name);
        }
    }

    protected abstract void doRun() throws Exception;

    protected void checkInterrupted() {
        if (isInterrupted())
            throw new ReplicaMapException("Interrupted.", new InterruptedException());

        // Bail out with the root cause if any worker of this manager has already failed, rather
        // than waiting for the interrupt the manager sends while stopping itself.
        if (manager != null)
            manager.checkNotFailed();
    }

    public boolean interrupt() {
        if (interrupted.compareAndSet(false, true)) {
            interruptThread();

            return true;
        }
        return false;
    }

    protected void interruptThread() {
        var th = this.thread;
        if (th != null)
            th.interrupt();
    }

    public boolean isInterrupted() {
        if (interrupted.get())
            return true;

        var th = thread;
        return th != null && th.isInterrupted();
    }

    /**
     * @return {@code true} if this worker's thread is started and still running.
     */
    public boolean isAlive() {
        var th = thread;
        return th != null && th.isAlive();
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

            var th = worker.thread;
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
