package com.vladykin.replicamap.kafka.impl.worker;

public interface WorkerThread<T extends Thread & WorkerThread<T>> {
    Worker getReplicaMapWorker();
    void start();
    void interrupt();
    boolean isInterrupted();
    boolean isAlive();
    void join() throws InterruptedException;
}
