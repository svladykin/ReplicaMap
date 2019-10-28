package com.vladykin.replicamap.kafka.impl.util;

import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import static com.vladykin.replicamap.kafka.impl.util.Utils.isOverMaxOffset;

/**
 * The queue that collects all the updated keys, that need to be flushed.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class FlushQueue {
    protected final Semaphore lock = new Semaphore(1);
    protected final ArrayDeque<ConsumerRecord<Object,OpMessage>> queue = new ArrayDeque<>();

    protected long maxAddOffset = -1;
    protected long maxCleanOffset = -1;

    protected final ThreadLocal<ArrayDeque<ConsumerRecord<Object,OpMessage>>> threadLocalQueue =
        ThreadLocal.withInitial(ArrayDeque::new);

    /**
     * @return Size of the internal queue.
     */
    public int size() {
        lock.acquireUninterruptibly();
        try {
            return queue.size();
        }
        finally {
            lock.release();
        }
    }

    /**
     * @param rec Operation record.
     * @param update If {@code true} then it was a successful map update,
     *               if {@code false} the it was a failed attempt or a flush record.
     * @param waitLock If {@code true} then current thread will wait for the lock acquisition,
     *                 if {@code false} and lock acquisition failed, then operation is allowed
     *                 to store the record into thread local buffer.
     */
    public void add(ConsumerRecord<Object,OpMessage> rec, boolean update, boolean waitLock) {
        Utils.requireNonNull(rec, "rec");

        ArrayDeque<ConsumerRecord<Object,OpMessage>> tlq = threadLocalQueue.get();

        if (lock(waitLock)) {
            try {
                assert rec.offset() > maxAddOffset: rec.offset();

                while (!tlq.isEmpty())
                    queue.add(tlq.poll());

                if (update)
                    queue.add(rec);

                maxAddOffset = rec.offset();
            }
            finally {
                lock.release();
            }
        }
        else if (update)
            tlq.add(rec);
    }

    protected boolean lock(boolean waitLock) {
        if (waitLock) {
            lock.acquireUninterruptibly();
            return true;
        }
        return lock.tryAcquire();
    }

    /**
     * Collects update records to the given batch.
     * Does not modify state.
     *
     * @param dataBatch Data batch to collect records into.
     * @param maxOffset Max offset.
     * @return Number of all collected records and number of collected updates as a single long value.
     * @see #collectedAll(long)
     * @see #collectedUpdates(long)
     */
    public long collectUpdateRecords(Map<Object,Object> dataBatch, long maxOffset) {
        assert dataBatch.isEmpty();

        lock.acquireUninterruptibly();
        try {
            int collectedUpdates = 0;
            for (ConsumerRecord<Object,OpMessage> rec : queue) {
                if (isOverMaxOffset(rec, maxOffset))
                    break;

                collectedUpdates++;
                dataBatch.put(rec.key(), rec.value().getUpdatedValue());
            }

            long collectedAll = Math.min(maxAddOffset, maxOffset) - maxCleanOffset;

            if (collectedAll <= 0) {
                assert collectedUpdates == 0;
                return 0L;
            }

            return (collectedAll << 32) | collectedUpdates;
        }
        finally {
            lock.release();
        }
    }

    public static int collectedUpdates(long collectUpdateRecordsResult) {
        return (int)collectUpdateRecordsResult;
    }

    public static int collectedAll(long collectUpdateRecordsResult) {
        return (int)(collectUpdateRecordsResult >>> 32);
    }

    /**
     * Cleans the flush queue for the collected updates.
     *
     * @param collectedUpdates Collected updates.
     * @param collectedAll Collected all events.
     * @return New cleaned offset of this flush queue.
     */
    public long cleanCollected(int collectedUpdates, int collectedAll) {
        assert collectedUpdates >= 0 && collectedAll >= collectedUpdates;

        lock.acquireUninterruptibly();
        try {
            for (int i = 0; i < collectedUpdates; i++)
                queue.poll();

            updateMaxCleanOffset(maxCleanOffset + collectedAll);

            return maxCleanOffset;
        }
        finally {
            lock.release();
        }
    }

    @SuppressWarnings("ConstantConditions")
    protected void updateMaxCleanOffset(long maxOffset) {
        assert maxOffset <= maxAddOffset || queue.isEmpty(): maxOffset + " " + maxAddOffset + " " + queue.isEmpty();
        maxCleanOffset = queue.isEmpty() ? maxAddOffset : maxOffset;
    }

    /**
     * Cleans the flush queue until the given max offset.
     * @param maxOffset Max offset.
     * @return Number of cleaned updates.
     */
    public int clean(long maxOffset) {
        lock.acquireUninterruptibly();
        try {
            int cleanedUpdates = 0;
            for (;;) {
                ConsumerRecord<Object,OpMessage> rec = queue.peek();

                if (rec == null || isOverMaxOffset(rec, maxOffset))
                    break;

                cleanedUpdates++;
                queue.poll();
            }

            updateMaxCleanOffset(maxOffset);

            return cleanedUpdates;
        }
        finally {
            lock.release();
        }
    }
}
