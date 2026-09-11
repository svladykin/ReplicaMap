package com.vladykin.replicamap.kafka.impl.worker;

/**
 * Thread used to run a {@link Worker}.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class DefaultWorkerThread extends Thread implements WorkerThread<DefaultWorkerThread> {
    private final Worker worker;

    public DefaultWorkerThread(Worker worker) {
        super(worker, worker.name);
        this.worker = worker;
    }

    @Override
    public Worker getReplicaMapWorker() {
        return worker;
    }
}
