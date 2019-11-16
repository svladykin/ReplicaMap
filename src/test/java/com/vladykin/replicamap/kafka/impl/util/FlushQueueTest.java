package com.vladykin.replicamap.kafka.impl.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

import static com.vladykin.replicamap.base.ReplicaMapBaseMultithreadedTest.executeThreads;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlushQueueTest {
    @Test
    void testSimple() {
        FlushQueue q = new FlushQueue(null);

        assertEquals(-1, q.maxCleanOffset);
        assertEquals(-1, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.add(null,null, 0, true, false);

        assertEquals(-1, q.maxCleanOffset);
        assertEquals(0, q.maxAddOffset);
        assertEquals(1, q.queue.size());

        FlushQueue.Batch batch = q.collect(iterator(1));
        assertNull(batch);

        batch = q.collect(iterator(0));
        int collectedAll = batch.getCollectedAll();

        assertEquals(1, q.size());
        assertEquals(1, batch.size());
        assertEquals(1, collectedAll);

        q.clean(0, "");

        assertEquals(0, q.maxCleanOffset);
        assertEquals(0, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.add(1,null, 1, true, false);
        q.add(2,null, 2, true, false);
        q.add(3,null, 3, true, false);
        q.add(4,null, 4, true, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(4, q.maxAddOffset);
        assertEquals(4, q.queue.size());

        q.add(5,null, 5, false, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(5, q.maxAddOffset);
        assertEquals(4, q.queue.size());

        q.add(6,null, 6, false, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(6, q.maxAddOffset);
        assertEquals(4, q.queue.size());

        q.add(7,null, 7, true, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(7, q.maxAddOffset);
        assertEquals(5, q.queue.size());

        batch = q.collect(iterator(7));
        collectedAll = batch.getCollectedAll();

        assertEquals(7, collectedAll);
        assertEquals(5, batch.size());
        assertEquals(new HashSet<>(Arrays.asList(1,2,3,4,7)), batch.keySet());

        assertEquals(0, q.maxCleanOffset);
        assertEquals(7, q.maxAddOffset);
        assertEquals(5, q.queue.size());

        q.clean(batch.getMaxOffset(), "");

        assertEquals(7, q.maxCleanOffset);
        assertEquals(7, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.add(8,null, 7, true, true);

        assertEquals(7, q.maxCleanOffset);
        assertEquals(7, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.add(9,null, 9, true, true);

        assertEquals(7, q.maxCleanOffset);
        assertEquals(9, q.maxAddOffset);
        assertEquals(1, q.queue.size());

         q.clean(6, "");

        assertEquals(7, q.maxCleanOffset);
        assertEquals(9, q.maxAddOffset);
        assertEquals(1, q.queue.size());

        System.out.println(q);

        q.clean(10, "");

        assertEquals(10, q.maxCleanOffset);
        assertEquals(10, q.maxAddOffset);
        assertEquals(0, q.queue.size());
    }

    LongStream iterator(long... x) {
        return LongStream.of(x);
    }

    @Test
    public void testThreadLocalBuffer() {
        FlushQueue q = new FlushQueue(null);

        q.lock.acquireUninterruptibly();

        q.add(null,null, 1, true, false);
        q.add(null,null, 2, true, false);
        q.add(null,null, 3, true, false);

        assertEquals(-1, q.maxCleanOffset);
        assertEquals(-1, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.lock.release();

        q.add(null,null, 4, true, true);

        assertEquals(-1, q.maxCleanOffset);
        assertEquals(4, q.maxAddOffset);
        assertEquals(4, q.queue.size());
    }

    @Test
    void testMultithreaded() throws Exception {
        ExecutorService exec = Executors.newCachedThreadPool();

        try {
            AtomicLong allAddedCnt = new AtomicLong();
            AtomicLong allCleanedCnt = new AtomicLong();

            AtomicLong lastAddedOffset = new AtomicLong(-1);

            FlushQueue q = new FlushQueue(null);

            for (int j = 0; j < 50; j++) {
                CyclicBarrier start = new CyclicBarrier(3);

                CompletableFuture<Object> addFut = executeThreads(1, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    start.await();

                    int cnt = 500_000;
                    for (int i = 1; i <= cnt; i++) {
                        boolean update = i == cnt || rnd.nextInt(10) == 0;
                        boolean waitLock = i == cnt || rnd.nextInt(20) == 0;

                        q.add(null,null, lastAddedOffset.incrementAndGet(), update, waitLock);

                        allAddedCnt.incrementAndGet();
                    }

                    return null;
                }).get(0);

                CompletableFuture<?> cleanFut = Utils.allOf(executeThreads(2, exec, () -> {
                    start.await();

                    while (!addFut.isDone() || q.size() > 0) {
                        FlushQueue.Batch batch = q.collect(iterator(lastAddedOffset.get()));

                        if (batch == null)
                            continue;

                        int collectedAll = batch.getCollectedAll();
                        assertTrue(collectedAll >= batch.size());

                        long cleanedCnt = q.clean(batch.getMaxOffset(), "");

                        allCleanedCnt.addAndGet(cleanedCnt);
                    }

                    return null;
                }));

                addFut.get(3, TimeUnit.SECONDS);
                cleanFut.get(3, TimeUnit.SECONDS);

//                assertEquals(updatesAddedCnt.get(), updatesCollectedCnt.get());
                assertEquals(allAddedCnt.get(), allCleanedCnt.get());
                assertTrue(q.queue.isEmpty());
                assertEquals(q.maxAddOffset, q.maxCleanOffset);
                System.out.println("iteration " + j + " OK");
            }
        }
        finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(3, TimeUnit.SECONDS));
        }
    }
}