package com.vladykin.replicamap.kafka.impl.util;

import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.OptionalLong;
import java.util.concurrent.Semaphore;
import java.util.stream.LongStream;
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
        assert rec != null;

        ArrayDeque<ConsumerRecord<Object,OpMessage>> tlq = threadLocalQueue.get();

        if (lock(waitLock)) {
            try {
                for (;;) {
                    ConsumerRecord<Object,OpMessage> r = tlq.poll();

                    if (r == null)
                        break;

                    if (isOverMaxOffset(r, maxAddOffset))
                        queue.add(r);
                }

                if (isOverMaxOffset(rec, maxAddOffset)) {
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
     * Collects records to the given batch.
     * Does not modify the state.
     *
     * @param maxOffsets Max offsets to collect (inclusive). The stream is expected to be sorted.
     * @return Collected batch.
     */
    public Batch collect(LongStream maxOffsets) {
        lock.acquireUninterruptibly();
        try {
            OptionalLong maxOffsetOptional = maxOffsets
                .filter(offset -> offset <= maxAddOffset)
                .max();

            if (!maxOffsetOptional.isPresent())
                return null;

            long maxOffset = maxOffsetOptional.getAsLong();
            Batch dataBatch = new Batch(maxOffset, maxCleanOffset);

            for (ConsumerRecord<Object,OpMessage> rec : queue) {
                if (isOverMaxOffset(rec, maxOffset))
                    break;

                dataBatch.put(rec.key(), rec.value().getUpdatedValue());
            }

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
        protected final long maxOffset;
        protected final int collectedAll;

        public Batch(long maxOffset, long maxCleanOffset) {
            this.maxOffset = maxOffset;
            this.collectedAll = (int)Math.max(0, maxOffset - maxCleanOffset);
        }

        public int getCollectedAll() {
            return collectedAll;
        }

        public long getMaxOffset() {
            return maxOffset;
        }

        @Override
        public String toString() {
            return "Batch{" +
                "maxOffset=" + maxOffset +
                ", collectedAll=" + collectedAll +
                ", map=" + super.toString() +
                '}';
        }
    }
}
