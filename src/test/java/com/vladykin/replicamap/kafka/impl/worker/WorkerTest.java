package com.vladykin.replicamap.kafka.impl.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerTest {
    @Test
    void testWorker() throws InterruptedException {
        int threads = 10;

        CyclicBarrier start = new CyclicBarrier(threads);
        CountDownLatch exit = new CountDownLatch(threads);

        AtomicLong cnt = new AtomicLong();
        List<Worker> workers = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            Worker w = new Worker("testGroup", i) {
                @Override
                protected void doRun() throws Exception {
                    start.await();
                    for (int i = 0; i < 10; i++)
                        cnt.incrementAndGet();

                    exit.countDown();
                }
            };

            assertTrue(w.getName().startsWith("testGroup"));
            assertTrue(w.getName().endsWith(String.valueOf(i)));
            assertTrue(w.toString().contains(w.getName()));

            assertNull(w.thread);
            w.start();
            assertNotNull(w.thread);

            workers.add(w);
        }

        assertTrue(exit.await(3, TimeUnit.SECONDS));

        Worker.interruptAll(null);
        Worker.joinAll(null);

        workers.add(null);
        Worker.interruptAll(workers);
        Worker.joinAll(workers);

        for (Worker w : workers)
            assertFalse(w != null && w.thread.isAlive());
    }

    @Test
    void testWorkerFailure() throws InterruptedException {
        AtomicBoolean executed = new AtomicBoolean();

        Worker w = new Worker("testGroup", 1) {
            @Override
            protected void doRun() {
                executed.set(true);
            }
        };

        assertFalse(w.isInterrupted());
        assertTrue(w.interrupt());
        assertFalse(w.interrupt());
        assertTrue(w.isInterrupted());

        w.start();

        w.thread.join(3000);

        assertThrows(IllegalStateException.class, w::start);
        assertFalse(executed.get());
    }

}