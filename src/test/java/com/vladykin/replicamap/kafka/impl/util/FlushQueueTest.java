package com.vladykin.replicamap.kafka.impl.util;

import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static com.vladykin.replicamap.base.ReplicaMapBase.OP_PUT;
import static com.vladykin.replicamap.base.ReplicaMapBaseMultithreadedTest.executeThreads;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlushQueueTest {
    @Test
    void testSimple() {
        FlushQueue q = new FlushQueue();

        assertEquals(-1, q.cleanCollected(0, 100));
        assertEquals(0, q.clean(100));

        assertEquals(-1, q.maxCleanOffset);
        assertEquals(-1, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.add(newRecord(0), true, false);

        assertEquals(-1, q.maxCleanOffset);
        assertEquals(0, q.maxAddOffset);
        assertEquals(1, q.queue.size());

        Map<Object,Object> m = new HashMap<>();
        long x = q.collectUpdateRecords(m, 5);
        int collectedUpdates = FlushQueue.collectedUpdates(x);
        int collectedAll = FlushQueue.collectedAll(x);

        assertEquals(1, collectedUpdates);
        assertEquals(1, collectedAll);

        assertEquals(1, q.clean(0));

        assertEquals(0, q.maxCleanOffset);
        assertEquals(0, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.add(newRecord(1), true, false);
        q.add(newRecord(2), true, false);
        q.add(newRecord(3), true, false);
        q.add(newRecord(4), true, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(4, q.maxAddOffset);
        assertEquals(4, q.queue.size());

        q.add(newRecord(5), false, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(5, q.maxAddOffset);
        assertEquals(4, q.queue.size());

        q.add(newRecord(6), false, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(6, q.maxAddOffset);
        assertEquals(4, q.queue.size());

        q.add(newRecord(7), true, false);

        assertEquals(0, q.maxCleanOffset);
        assertEquals(7, q.maxAddOffset);
        assertEquals(5, q.queue.size());

        m = new HashMap<>();
        x = q.collectUpdateRecords(m, 5);
        collectedUpdates = FlushQueue.collectedUpdates(x);
        collectedAll = FlushQueue.collectedAll(x);

        assertEquals(4, collectedUpdates);
        assertEquals(5, collectedAll);
        assertEquals(4, m.size());
        assertEquals(new HashSet<>(Arrays.asList(1L,2L,3L,4L)), m.keySet());

        assertEquals(0, q.maxCleanOffset);
        assertEquals(7, q.maxAddOffset);
        assertEquals(5, q.queue.size());

        assertEquals(5, q.cleanCollected(collectedUpdates, collectedAll));

        assertEquals(5, q.maxCleanOffset);
        assertEquals(7, q.maxAddOffset);
        assertEquals(1, q.queue.size());

        m = new HashMap<>();
        x = q.collectUpdateRecords(m, 5);
        collectedUpdates = FlushQueue.collectedUpdates(x);
        collectedAll = FlushQueue.collectedAll(x);

        assertEquals(0, collectedUpdates);
        assertEquals(0, collectedAll);
        assertTrue(m.isEmpty());

        assertEquals(5, q.maxCleanOffset);
        assertEquals(7, q.maxAddOffset);
        assertEquals(1, q.queue.size());

        assertEquals(5, q.cleanCollected(collectedUpdates, collectedAll));

        assertEquals(5, q.maxCleanOffset);
        assertEquals(7, q.maxAddOffset);
        assertEquals(1, q.queue.size());

        q.add(newRecord(8), true, false);
        q.add(newRecord(9), true, false);

        assertEquals(5, q.maxCleanOffset);
        assertEquals(9, q.maxAddOffset);
        assertEquals(3, q.queue.size());

        m = new HashMap<>();
        x = q.collectUpdateRecords(m, 10);
        collectedUpdates = FlushQueue.collectedUpdates(x);
        collectedAll = FlushQueue.collectedAll(x);

        assertEquals(3, collectedUpdates);
        assertEquals(4, collectedAll);
        assertEquals(3, m.size());

        assertEquals(5, q.maxCleanOffset);
        assertEquals(9, q.maxAddOffset);
        assertEquals(3, q.queue.size());

        assertEquals(9, q.cleanCollected(collectedUpdates, collectedAll));

        assertEquals(9, q.maxCleanOffset);
        assertEquals(9, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.add(newRecord(10), true, false);
        q.add(newRecord(11), true, false);
        q.add(newRecord(12), false, false);

        assertEquals(9, q.maxCleanOffset);
        assertEquals(12, q.maxAddOffset);
        assertEquals(2, q.queue.size());

        // Race between collect and clean
        m = new HashMap<>();
        x = q.collectUpdateRecords(m, 15);
        collectedUpdates = FlushQueue.collectedUpdates(x);
        collectedAll = FlushQueue.collectedAll(x);

        assertEquals(2, collectedUpdates);
        assertEquals(3, collectedAll);
        assertEquals(2, m.size());

        q.add(newRecord(13), true, false);
        q.add(newRecord(14), false, false);
        q.add(newRecord(15), true, false);

        assertEquals(9, q.maxCleanOffset);
        assertEquals(15, q.maxAddOffset);
        assertEquals(4, q.queue.size());

        assertEquals(12, q.cleanCollected(collectedUpdates, collectedAll));

        assertEquals(12, q.maxCleanOffset);
        assertEquals(15, q.maxAddOffset);
        assertEquals(2, q.queue.size());

        m = new HashMap<>();
        x = q.collectUpdateRecords(m, 5);
        collectedUpdates = FlushQueue.collectedUpdates(x);
        collectedAll = FlushQueue.collectedAll(x);

        assertEquals(0, collectedUpdates);
        assertEquals(0, collectedAll);
        assertEquals(0, m.size());

        q.clean(12);

        assertEquals(12, q.maxCleanOffset);
        assertEquals(15, q.maxAddOffset);
        assertEquals(2, q.queue.size());

        q.clean(13);

        assertEquals(13, q.maxCleanOffset);
        assertEquals(15, q.maxAddOffset);
        assertEquals(1, q.queue.size());

        q.clean(17);

        assertEquals(15, q.maxCleanOffset);
        assertEquals(15, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.clean(17);

        assertEquals(15, q.maxCleanOffset);
        assertEquals(15, q.maxAddOffset);
        assertEquals(0, q.queue.size());
    }

    @Test
    public void testThreadLocalBuffer() {
        FlushQueue q = new FlushQueue();

        q.lock.acquireUninterruptibly();

        q.add(newRecord(1), true, false);
        q.add(newRecord(2), true, false);
        q.add(newRecord(3), true, false);

        assertEquals(-1, q.maxCleanOffset);
        assertEquals(-1, q.maxAddOffset);
        assertEquals(0, q.queue.size());

        q.lock.release();

        q.add(newRecord(4), true, true);

        assertEquals(-1, q.maxCleanOffset);
        assertEquals(4, q.maxAddOffset);
        assertEquals(4, q.queue.size());
    }

    @Test
    void testMultithreaded() throws Exception {
        ExecutorService exec = Executors.newCachedThreadPool();

        try {
            for (int j = 0; j < 50; j++) {
                FlushQueue q = new FlushQueue();
                CyclicBarrier start = new CyclicBarrier(2);

                long maxAdds = 500_000;
                long baseOffset = j % 3;

                AtomicLong addedOffset = new AtomicLong(baseOffset);
                AtomicLong collectedOffset = new AtomicLong();

                AtomicLong updatesAddedCnt = new AtomicLong();
                AtomicLong updatesCollectedCnt = new AtomicLong();

                CompletableFuture<Object> addFut = executeThreads(1, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    start.await();

                    long lastAddOffset = baseOffset + maxAdds - 1;

                    for (long i = baseOffset; i < lastAddOffset; i++) {
                        boolean update = rnd.nextInt(10) == 0;
                        boolean waitLock = rnd.nextInt(20) == 0;

                        q.add(newRecord(i), update, waitLock);
                        if (update)
                            updatesAddedCnt.incrementAndGet();
                        addedOffset.incrementAndGet();
                    }

                    q.add(newRecord(lastAddOffset), false, true);
                    addedOffset.incrementAndGet();

                    return null;
                }).get(0);

                CompletableFuture<Object> cleanFlushFut = executeThreads(1, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    start.await();

                    while (collectedOffset.get() < baseOffset + maxAdds) {
                        // we can read this field safely because it is modified only in current thread
                        long oldCleanOffset = q.maxCleanOffset;
                        long newCleanOffset;

                        if (rnd.nextBoolean()) {
                            updatesCollectedCnt.addAndGet(q.clean(addedOffset.get()));
                            newCleanOffset = q.maxCleanOffset;
                        }
                        else {
                            Map<Object,Object> m = new HashMap<>();
                            long x = q.collectUpdateRecords(m, addedOffset.get());
                            int collectedUpdates = FlushQueue.collectedUpdates(x);
                            int collectedAll = FlushQueue.collectedAll(x);

                            assertEquals(collectedUpdates, m.size());
                            assertTrue(collectedAll >= collectedUpdates);

                            updatesCollectedCnt.addAndGet(collectedUpdates);
                            newCleanOffset = q.cleanCollected(collectedUpdates, collectedAll);
                            // collectedAll here can be less than the actual cleaned count
                        }

                        collectedOffset.addAndGet(newCleanOffset - oldCleanOffset);
                    }

                    return null;
                }).get(0);

                addFut.get(3, TimeUnit.SECONDS);
                cleanFlushFut.get(3, TimeUnit.SECONDS);

                assertTrue(updatesCollectedCnt.get() > 0);
                assertTrue(updatesCollectedCnt.get() < maxAdds);
                assertEquals(updatesAddedCnt.get(), updatesCollectedCnt.get());
                assertEquals(baseOffset + maxAdds, addedOffset.get());
                assertEquals(baseOffset + maxAdds, collectedOffset.get());

                System.out.println("iteration " + j + " OK");
            }
        }
        finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    public static ConsumerRecord<Object,OpMessage> newRecord(long offset) {
        return new ConsumerRecord<>("", 0, offset, offset, new OpMessage(OP_PUT, 777, 100500, null, offset));
    }
}