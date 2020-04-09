package com.vladykin.replicamap.kafka.impl;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.OptionalLong;
import java.util.concurrent.Semaphore;
import java.util.stream.LongStream;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vladykin.replicamap.kafka.impl.util.Utils.isOverMaxOffset;

/**
 * The queue that collects all the updated keys and values to be flushed.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class FlushQueue {
    private static final Logger log = LoggerFactory.getLogger(FlushQueue.class);

    protected final Semaphore lock = new Semaphore(1);
    protected final ArrayDeque<MiniRecord> queue = new ArrayDeque<>();

    protected final TopicPartition dataPart;

    protected long maxAddOffset = -1;
    protected long maxCleanOffset = -1;

    protected final ThreadLocal<ArrayDeque<MiniRecord>> threadLocalQueue =
        ThreadLocal.withInitial(ArrayDeque::new);

    public FlushQueue(TopicPartition dataPart) {
        this.dataPart = dataPart;
    }

    public TopicPartition getDataPartition() {
        return dataPart;
    }

    @Override
    public String toString() {
        return "FlushQueue{" +
            "dataPart=" + dataPart +
            ", locked=" + (lock.availablePermits() == 0) +
            '}';
    }

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
     * @param key Key or {@code null} if it is not an update or unsuccessful update attempt.
     * @param value Value.
     * @param offset Offset.
     * @param waitLock If {@code true} then current thread will wait for the lock acquisition,
     *                 if {@code false} and lock acquisition failed, then operation is allowed
     *                 to store the record into thread local buffer.
     */
    public void add(Object key, Object value, long offset, boolean waitLock) {
        ArrayDeque<MiniRecord> tlq = threadLocalQueue.get();

        if (!lock(waitLock)) {
            tlq.add(new MiniRecord(key, value, offset));
            return;
        }

        try {
            for (;;) {
                MiniRecord r = tlq.poll();

                if (r == null)
                    break;

                addRecord(r);
            }

            addRecord(new MiniRecord(key, value, offset));
        }
        finally {
            lock.release();
        }
    }

    protected void addRecord(MiniRecord rec) {
        if (maxAddOffset == -1)
            maxAddOffset = rec.offset();
        else {
            long nextOffset = maxAddOffset + 1;

            if (nextOffset != rec.offset()) // check that we do not miss any records
                throw new IllegalStateException("Expected record offset " + nextOffset + ", actual " + rec.offset());

            maxAddOffset = nextOffset;
        }

        if (rec.key() == null) // non-update record
            return;

        if (log.isTraceEnabled())
            log.trace("For partition {} add record: {}", dataPart, rec);

        queue.add(rec);
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
            if (queue.isEmpty())
                return null;

            OptionalLong maxOffsetOptional = maxOffsets
                .filter(offset -> offset <= maxAddOffset)
                .max();

            if (!maxOffsetOptional.isPresent())
                return null;

            long maxOffset = maxOffsetOptional.getAsLong();
            long minOffset = queue.peek().offset();

            if (minOffset > maxOffset)
                return null;

            Batch dataBatch = new Batch(minOffset, maxOffset, maxCleanOffset);

            for (MiniRecord rec : queue) {
                if (isOverMaxOffset(rec, maxOffset))
                    break;

                dataBatch.put(rec.key(), rec.value());
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
     * @param reason The context of this cleaning.
     * @return Actual number of cleaned events.
     */
    public long clean(long maxOffset, String reason) {
        lock.acquireUninterruptibly();
        try {
            if (maxCleanOffset >= maxOffset)
                return 0;

//            trace.trace("clean {} {} maxOffset={}, maxCleanOffset={}, maxAddOffset={}",
//                reason, dataPart, maxOffset, maxCleanOffset, maxAddOffset);

            for (;;) {
                MiniRecord rec = queue.peek();

                if (rec == null || isOverMaxOffset(rec, maxOffset))
                    break;

                queue.poll();
            }

            long cleanedCnt = maxOffset - maxCleanOffset;

            if (log.isDebugEnabled()) {
                log.debug("For partition {} clean maxCleanOffset: {} -> {}, reason: {}",
                    dataPart, maxCleanOffset, maxOffset, reason);
            }

            maxCleanOffset = maxOffset;

            if (maxCleanOffset > maxAddOffset) {
                assert queue.isEmpty();

                if (log.isDebugEnabled())
                    log.debug("For partition {} clean maxAddOffset: {} -> {}", dataPart, maxAddOffset, maxCleanOffset);

                maxAddOffset = maxCleanOffset;
            }

            return cleanedCnt;
        }
        finally {
            lock.release();
        }
    }

    public static class Batch extends HashMap<Object,Object> {
        protected final long minOffset;
        protected final long maxOffset;
        protected final long maxCleanOffset;

        public Batch(long minOffset, long maxOffset, long maxCleanOffset) {
            this.minOffset = minOffset;
            this.maxOffset = maxOffset;
            this.maxCleanOffset = maxCleanOffset;
        }

        public int getCollectedAll() {
            return (int)Math.max(0, maxOffset - maxCleanOffset);
        }

        public long getMinOffset() {
            return minOffset;
        }

        public long getMaxOffset() {
            return maxOffset;
        }

        @SuppressWarnings("unused")
        public long getMaxCleanOffset() {
            return maxCleanOffset;
        }

        @Override
        public String toString() {
            return "Batch{" +
                "minOffset=" + minOffset +
                ", maxOffset=" + maxOffset +
                ", maxCleanOffset=" + maxCleanOffset +
                ", collectedAll=" + getCollectedAll() +
                ", size=" + size() +
                ", map=" + super.toString() +
                '}';
        }
    }
}
