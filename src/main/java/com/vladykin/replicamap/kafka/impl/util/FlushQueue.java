package com.vladykin.replicamap.kafka.impl.util;

import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import java.util.ArrayDeque;
import java.util.HashMap;
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
                for (;;) {
                    ConsumerRecord<Object,OpMessage> r = tlq.poll();

                    if (r == null)
                        break;

                    if (r.offset() > maxAddOffset)
                        queue.add(r);
                }

                if (rec.offset() > maxAddOffset) {
                    maxAddOffset = rec.offset();

                    if (update)
                        queue.add(rec);
                }
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
     * Does not modify the state.
     *
     * @return Collected batch.
     */
    public Batch collectUpdateRecords() {
        lock.acquireUninterruptibly();
        try {
            Batch dataBatch = new Batch(maxAddOffset, maxCleanOffset);

            for (ConsumerRecord<Object,OpMessage> rec : queue)
                dataBatch.put(rec.key(), rec.value().getUpdatedValue());

            return dataBatch;
        }
        finally {
            lock.release();
        }
    }

    /**
     * Cleans the flush queue until the given max offset.
     * @param maxOffset Max offset.
     * @return Actual number of cleaned events.
     */
    public long clean(long maxOffset) {
        lock.acquireUninterruptibly();
        try {
            if (maxCleanOffset >= maxOffset)
                return 0;

            for (;;) {
                ConsumerRecord<Object,OpMessage> rec = queue.peek();

                if (rec == null || isOverMaxOffset(rec, maxOffset))
                    break;

                queue.poll();
            }

            long cleanedCnt = maxOffset - maxCleanOffset;
            maxCleanOffset = maxOffset;

            if (maxCleanOffset > maxAddOffset) {
                assert queue.isEmpty();
                maxAddOffset = maxCleanOffset;
            }

            return cleanedCnt;
        }
        finally {
            lock.release();
        }
    }

    public static class Batch extends HashMap<Object,Object> {
        protected final long maxAddOffset;
        protected final long maxCleanOffset;

        public Batch(long maxAddOffset, long maxCleanOffset) {
            this.maxAddOffset = maxAddOffset;
            this.maxCleanOffset = maxCleanOffset;
        }

        public int getCollectedAll() {
            return (int)(maxAddOffset - maxCleanOffset);
        }

        public long getMaxOffset() {
            return maxAddOffset;
        }
    }
}
