package com.vladykin.replicamap.kafka.impl.worker.flush;

import com.vladykin.replicamap.kafka.impl.util.Utils;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlushQueueTest {
    @Test
    void testSimple() {
        FlushQueue q = new FlushQueue(null);
        q.setMaxOffset(-1);

        assertEquals(-1, q.maxCleanOffset);
        assertEquals(-1, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.add(1,null, 0, false);

        assertEquals(-1, q.maxCleanOffset);
        assertEquals(0, q.maxAddOffset);
        assertEquals(1, q.queue.size());

        FlushQueue.Batch batch = q.collect(stream(1));
        assertNull(batch);

        batch = q.collect(stream(0));
        int collectedAll = batch.getCollectedAll();

        assertEquals(1, q.size());
        assertEquals(1, batch.size());
        assertEquals(1, collectedAll);

        q.clean(0, "");

        assertEquals(0, q.maxCleanOffset);
        assertEquals(0, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.add(1,null, 1, false);
        q.add(2,null, 2, false);
        q.add(3,null, 3, false);
        q.add(4,null, 4, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(4, q.maxAddOffset);
        assertEquals(4, q.queue.size());

        q.add(null,null, 5, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(5, q.maxAddOffset);
        assertEquals(4, q.queue.size());

        q.add(null,null, 6, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(6, q.maxAddOffset);
        assertEquals(4, q.queue.size());

        q.add(7,null, 7, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(7, q.maxAddOffset);
        assertEquals(5, q.queue.size());

        batch = q.collect(stream(7));
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

        assertThrows(IllegalStateException.class, () ->
            q.add(8,null, 7, true));

        assertEquals(7, q.maxCleanOffset);
        assertEquals(7, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.add(9,null, 8, true);

        assertEquals(7, q.maxCleanOffset);
        assertEquals(8, q.maxAddOffset);
        assertEquals(1, q.queue.size());

         q.clean(6, "");

        assertEquals(7, q.maxCleanOffset);
        assertEquals(8, q.maxAddOffset);
        assertEquals(1, q.queue.size());

        System.out.println(q);

        q.clean(10, "");

        assertEquals(10, q.maxCleanOffset);
        assertEquals(10, q.maxAddOffset);
        assertEquals(0, q.queue.size());
    }

    LongStream stream(long... x) {
        return LongStream.of(x);
    }

    @Test
    public void testThreadLocalBuffer() {
        FlushQueue q = new FlushQueue(null);

        q.setMaxOffset(0);

        q.lock.acquireUninterruptibly();

        q.add(1,null, 1, false);
        q.add(1,null, 2, false);
        q.add(1,null, 3, false);

        assertEquals(-1, q.maxCleanOffset);
        assertEquals(0, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.lock.release();

        q.add(1,null, 4, true);

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
            q.setMaxOffset(-1);

            for (int j = 0; j < 50; j++) {
                CyclicBarrier start = new CyclicBarrier(3);

                CompletableFuture<Object> addFut = executeThreads(1, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    start.await();

                    int cnt = 500_000;
                    for (int i = 1; i <= cnt; i++) {
                        boolean update = i == cnt || rnd.nextInt(10) == 0;
                        boolean waitLock = i == cnt || rnd.nextInt(20) == 0;

                        q.add(update ? i : null,null, lastAddedOffset.incrementAndGet(), waitLock);

                        allAddedCnt.incrementAndGet();
                    }

                    return null;
                }).get(0);

                CompletableFuture<?> cleanFut = Utils.allOf(executeThreads(2, exec, () -> {
                    start.await();

                    while (!addFut.isDone() || q.size() > 0) {
                        FlushQueue.Batch batch = q.collect(stream(lastAddedOffset.get()));

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

    @Test
    void testCollect() {
        FlushQueue q = new FlushQueue(null);
        q.setMaxOffset(1000);

        FlushQueue.Batch batch = q.collect(stream(1000L));

        assertNull(batch);

        q.add(1, 0, 1001, true);
        q.add(2, 0, 1002, true);
        q.add(null, null, 1003, true);
        q.add(3, 0, 1004, true);
        q.add(4, 0, 1005, true);

        batch = q.collect(stream(1000L));

        assertNull(batch);

        batch = q.collect(stream(1001L));

        assertEquals(1001, batch.getMinOffset());
        assertEquals(1001, batch.getMaxOffset());
        assertEquals(1, batch.size());
        assertTrue(batch.containsKey(1));

        batch = q.collect(stream(1003L));

        assertEquals(1001, batch.getMinOffset());
        assertEquals(1003, batch.getMaxOffset());
        assertEquals(2, batch.size());
        assertTrue(batch.containsKey(1));
        assertTrue(batch.containsKey(2));

        batch = q.collect(stream(1001, 1003L));

        assertEquals(1001, batch.getMinOffset());
        assertEquals(1003, batch.getMaxOffset());
        assertEquals(2, batch.size());
        assertTrue(batch.containsKey(1));
        assertTrue(batch.containsKey(2));


        batch = q.collect(stream(1004L));

        assertEquals(1001, batch.getMinOffset());
        assertEquals(1004, batch.getMaxOffset());
        assertEquals(3, batch.size());
        assertTrue(batch.containsKey(1));
        assertTrue(batch.containsKey(2));
        assertTrue(batch.containsKey(3));


        batch = q.collect(stream(1005L));

        assertEquals(1001, batch.getMinOffset());
        assertEquals(1005, batch.getMaxOffset());
        assertEquals(4, batch.size());
        assertTrue(batch.containsKey(1));
        assertTrue(batch.containsKey(2));
        assertTrue(batch.containsKey(3));
        assertTrue(batch.containsKey(4));

        batch = q.collect(stream(1006L));

        assertNull(batch);
    }

    @Test
    void testChecks() {
        FlushQueue q = new FlushQueue(null);

        assertThrows(IllegalStateException.class, () -> q.add("key", "val", 7, true));
        assertThrows(IllegalArgumentException.class, () -> q.setMaxOffset(-2));
        q.setMaxOffset(-1);
        assertThrows(IllegalStateException.class, () -> q.setMaxOffset(5));
    }
}