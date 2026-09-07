package com.vladykin.replicamap.kafka.impl.worker.flush;

import com.vladykin.replicamap.kafka.impl.msg.FlushNotification;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import com.vladykin.replicamap.kafka.impl.util.NonReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The queue that collects all the updated keys and values to be flushed.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class FlushQueue {
    private static final Logger log = LoggerFactory.getLogger(FlushQueue.class);

    protected static final long NOT_INITIALIZED = -2;

    protected final TopicPartition dataPart;
    protected final NonReentrantLock lock = new NonReentrantLock();

    protected final ArrayDeque<UpdateRec> unflushedUpdates = new ArrayDeque<>();
    protected long maxAddedOpsOffset = NOT_INITIALIZED;

    /** De-spam: the last offset the time-based trigger already requested a flush for. */
    protected long lastStaleFlushRequestOffset = -1;

    /** Whether this partition's flushing is currently owned locally (the idle-flush trigger gate).
     * Written by the flush-worker thread on (re)assignment, read by the ops-worker thread. */
    protected volatile boolean flushOwned;

    /** Resume base: the flushed ops offset the loaded snapshot is consistent with (-1 if never flushed). */
    protected long resumeOpsOffset = -1;

    /** Whether we replayed the flush notification matching {@link #resumeOpsOffset}. */
    protected boolean foundResumeFlushNotification;

    protected final ArrayDeque<FlushRequestRec> flushRequests = new ArrayDeque<>();

    public FlushQueue(TopicPartition dataPart) {
        this.dataPart = dataPart;
    }

    public TopicPartition getDataPartition() {
        return dataPart;
    }

    /**
     * Sets whether this partition's flushing is owned by the local flush worker. Only the owner runs
     * the time-based idle-flush trigger, to avoid a herd of identical requests from every client.
     */
    public void setFlushOwned(boolean flushOwned) {
        this.flushOwned = flushOwned;
    }

    /**
     * @return {@code true} if this partition's flushing is currently owned locally.
     */
    public boolean isFlushOwned() {
        return flushOwned;
    }

    @Override
    public String toString() {
        if (!lock.tryLock()) {
            return "FlushQueue{" +
                    "dataPart=" + dataPart +
                    '}';
        }
        try (lock) {
            return "FlushQueue{" +
                    "dataPart=" + dataPart +
                    ", unflushedUpdates=" + unflushedUpdates.size() +
                    ", maxFlushedOpsOffset=" + getMaxFlushedOpsOffset() +
                    ", maxUnflushedOpsOffset=" + maxAddedOpsOffset +
                    ", flushRequests=" + flushRequests.size() +
                    '}';
        }
    }

    /**
     * @return Size of the internal unflushed updates queue.
     */
    public int unflushedUpdatesSize() {
        lock.lock();
        try (lock) {
            return unflushedUpdates.size();
        }
    }

    public long addOpsRecord(
        Object key,
        Object value,
        long offset,
        boolean successfulUpdate,
        FlushNotification flushNotification
    ) {
        return addOpsRecord(key, value, offset, successfulUpdate, flushNotification, System.nanoTime());
    }

    public long addOpsRecord(
        Object key,
        Object value,
        long offset,
        boolean successfulUpdate,
        FlushNotification flushNotification,
        long currentTimeNanos
    ) {
        lock.lock();
        try (lock) {
            long oldMaxUnflushedOpsOffset = maxAddedOpsOffset;

            if (oldMaxUnflushedOpsOffset == NOT_INITIALIZED || offset <= oldMaxUnflushedOpsOffset) {
                throw new IllegalStateException(
                        oldMaxUnflushedOpsOffset == NOT_INITIALIZED ?
                            "Need to init maxUnflushedOffset." :
                            "Record offset " + offset + " is not greater than max " + oldMaxUnflushedOpsOffset);
            }
            maxAddedOpsOffset = offset;

            if (flushNotification != null) {
                log.trace("For partition {} processing flush notification {}", dataPart, flushNotification);

                checkResumeFlushNotification(flushNotification);

                // Cleanup all the flushed updates from the queue.
                // Here we have all the flushed updates because we get the notification from the same ops partition,
                // e.g. all the needed updates are guaranteed to reside in the log before that notification gets into the log.
                cleanupFlushedUpdates(flushNotification.getOpsOffset());
            } else if (successfulUpdate) {
                var rec = new UpdateRec(key, value, offset, currentTimeNanos);
                log.trace("For partition {} adding update record: {}", dataPart, rec);
                unflushedUpdates.addLast(rec);
            }
            cleanupFlushRequests();
            return oldMaxUnflushedOpsOffset;
        }
    }

    public long staleFlushOffset(long maxUnflushedNanos) {
        return staleFlushOffset(System.nanoTime(), maxUnflushedNanos);
    }

    /**
     * Used by the time-based flush trigger for idle partitions. Returns each stale tail at most once
     * (it records the requested offset), so the caller does not re-request the same flush every poll;
     * a later tail (a higher offset) re-arms it. Once a request is sent, completing the flush is the
     * flush worker's job (it retries on its own), so there is no need to re-request here; the only
     * re-arm is {@link #onStaleFlushRequestFailed} for a request that failed to even be sent.
     *
     * @param currentTimeNanos Current nanoTime.
     * @param maxUnflushedNanos Max age the oldest unflushed update may reach before a flush is due.
     * @return Ops offset to request a flush up to (the current max added offset), or {@code -1} if the
     *         oldest unflushed update is younger than {@code maxUnflushedNanos}, there are none, or a
     *         flush was already requested for this offset.
     */
    public long staleFlushOffset(long currentTimeNanos, long maxUnflushedNanos) {
        lock.lock();
        try (lock) {
            UpdateRec first = unflushedUpdates.peekFirst();

            if (first == null || currentTimeNanos - first.createdAtNanos < maxUnflushedNanos)
                return -1;

            if (maxAddedOpsOffset <= lastStaleFlushRequestOffset)
                return -1; // Already requested a flush for this tail; wait for it (or a newer one).

            return lastStaleFlushRequestOffset = maxAddedOpsOffset;
        }
    }

    /**
     * Re-arms the time-based flush trigger when the flush request for the given offset failed to be
     * sent (called from the producer I/O callback thread). Without this the de-spam in
     * {@link #staleFlushOffset} would suppress any retry until a newer update arrives — possibly never
     * on an idle partition. No-op if a newer tail was requested meanwhile (the offset only grows).
     */
    public void onStaleFlushRequestFailed(long offset) {
        lock.lock();
        try (lock) {
            if (lastStaleFlushRequestOffset == offset)
                lastStaleFlushRequestOffset = -1;
        }
    }

    protected long getMaxFlushedOpsOffset() {
        assert maxAddedOpsOffset != NOT_INITIALIZED;
        return unflushedUpdates.isEmpty() ? maxAddedOpsOffset : (unflushedUpdates.peek().offset - 1);
    }

    /**
     * Set up the unflushed ops offset before adding records, arming the resume-consistency assertion
     * against the same offset.
     *
     * @param offset The last processed (flushed) ops offset the loaded snapshot is consistent with.
     */
    public void initUnflushedOpsOffset(long offset) {
        initUnflushedOpsOffset(offset, offset);
    }

    /**
     * Set up the unflushed ops offset before adding records.
     *
     * @param lastProcessedOffset The last processed ops offset (records are added from the next one).
     * @param resumeOpsOffset The flushed ops offset to arm the resume-consistency assertion with
     *                        ({@link #checkResumeFlushNotification}), or {@code -1} to disarm it —
     *                        never flushed, or the resume flush notification is known to not be
     *                        replayed (it lies in a verified-skipped retention-deleted range).
     */
    public void initUnflushedOpsOffset(long lastProcessedOffset, long resumeOpsOffset) {
        if (lastProcessedOffset <= NOT_INITIALIZED)
            throw new IllegalArgumentException("Illegal offset: " + lastProcessedOffset);

        lock.lock();
        try (lock) {
            if (maxAddedOpsOffset != NOT_INITIALIZED)
                throw new IllegalStateException("Max offset is already set: " + maxAddedOpsOffset);

            maxAddedOpsOffset = lastProcessedOffset;
            this.resumeOpsOffset = resumeOpsOffset;
        }
    }

    /**
     * Defense-in-depth (on top of the load convergence in {@code OpsWorker}): the flush notifications we
     * replay must reach exactly the resume offset before any later flush. A notification past the resume
     * offset before the resume offset itself means a flush the loaded snapshot already reflects sits
     * beyond where we resumed — snapshot and resume point disagree. Called under {@link #lock} from
     * {@link #addOpsRecord}.
     */
    protected void checkResumeFlushNotification(FlushNotification flushNotification) {
        long notificationOpsOffset = flushNotification.getOpsOffset();
        if (resumeOpsOffset >= 0 && !foundResumeFlushNotification) {
            if (notificationOpsOffset > resumeOpsOffset) {
                throw new IllegalStateException("Flush notification opsOffset " + notificationOpsOffset +
                    " is past the resume offset " + resumeOpsOffset + " for partition " + dataPart +
                    " before the resume flush was replayed: loaded snapshot and resume point disagree");
            }

            if (notificationOpsOffset == resumeOpsOffset)
                foundResumeFlushNotification = true;
        }
    }

    /**
     * Defense-in-depth check that we actually replayed the flush notification for the resume offset by
     * the time the partition reaches the steady state (we must, since that notification sits ahead of
     * where we resumed).
     */
    public void checkFoundResumeFlushNotification() {
        lock.lock();
        try (lock) {
            if (resumeOpsOffset >= 0 && !foundResumeFlushNotification) {
                throw new IllegalStateException("Reached steady state for partition " + dataPart +
                    " without replaying the resume flush notification for opsOffset " + resumeOpsOffset);
            }
        }
    }

    protected void cleanupFlushedUpdates(long flushedOpsOffset) {
        UpdateRec updateRec;
        while ((updateRec = unflushedUpdates.peekFirst()) != null && updateRec.offset <= flushedOpsOffset)
            unflushedUpdates.pollFirst();
    }

    protected void cleanupFlushRequests() {
        cleanupFlushRequests(getMaxFlushedOpsOffset());
    }

    protected void cleanupFlushRequests(long maxFlushedOpsOffset) {
        FlushRequestRec firstFlushReq;
        while ((firstFlushReq = flushRequests.peekFirst()) != null && firstFlushReq.opsOffset <= maxFlushedOpsOffset)
            flushRequests.pollFirst(); // Drop all the old flush requests that are below minUnflushedOpsOffset.
    }

    protected void appendNewFlushRequests(List<ConsumerRecord<Object, FlushRequest>> newFlushRequests, long maxFlushedOpsOffset) {
        if (newFlushRequests.isEmpty())
            return;

        var lastFlushReq = flushRequests.peekLast();
        long lastAddedFlushReqOpsOffset = -1;

        if (lastFlushReq != null)
            lastAddedFlushReqOpsOffset = lastFlushReq.opsOffset;

        for (var rec : newFlushRequests) {
            long opsOffset = rec.value().getOpsOffset();

            // Bump the out-of-order flush request ops offset to the current max to keep the queue
            // in strict ascending order and use the latest flush request record offset for commit.
            if (opsOffset < lastAddedFlushReqOpsOffset)
                opsOffset = lastAddedFlushReqOpsOffset;

            if (opsOffset > maxFlushedOpsOffset) {
                if (opsOffset == lastAddedFlushReqOpsOffset)
                    flushRequests.pollLast(); // Avoid duplicates.

                assert flushRequests.isEmpty() || flushRequests.getLast().offset < rec.offset();
                flushRequests.addLast(new FlushRequestRec(opsOffset, rec.offset()));
                lastAddedFlushReqOpsOffset = opsOffset;
            }
        }
    }

    /**
     * @return Collected batch.
     */
    public Batch collectBatch(List<ConsumerRecord<Object, FlushRequest>> newFlushRequests) {
        lock.lock();
        try (lock) {
            if (maxAddedOpsOffset == NOT_INITIALIZED)
                throw new IllegalStateException("Need to init maxUnflushedOpsOffset.");

            long maxFlushedOpsOffset = getMaxFlushedOpsOffset();
            cleanupFlushRequests(maxFlushedOpsOffset);
            appendNewFlushRequests(newFlushRequests, maxFlushedOpsOffset);

            if (unflushedUpdates.isEmpty() || flushRequests.isEmpty())
                return null;

            long maxReadyOpsOffset = -1;
            long maxReadyFlushOffset = -1;

            for (var flushReq : flushRequests) {
                if (flushReq.opsOffset > maxAddedOpsOffset)
                    break;

                maxReadyOpsOffset = flushReq.opsOffset;
                assert maxReadyOpsOffset >= maxFlushedOpsOffset; // We clear earlier and do not add more like this.

                maxReadyFlushOffset = flushReq.offset;
            }

            if (maxReadyOpsOffset == -1)
                return null;

            var dataBatch = new Batch(maxReadyOpsOffset, maxReadyFlushOffset);

            for (var rec : unflushedUpdates) {
                if (rec.offset > maxReadyOpsOffset)
                    break;

                dataBatch.put(rec.key, rec.value);
            }
            return dataBatch;
        }
    }

    public void resetFlushRequests() {
        lock.lock();
        try (lock) {
            flushRequests.clear();
        }
    }

    public class Batch implements Iterable<Map.Entry<Object,Object>> {
        protected final long opsOffset;
        protected final long flushRequestOffset;

        protected final Map<Object,Object> data = new HashMap<>();

        protected Batch(long opsOffset, long flushRequestOffset) {
            this.opsOffset = opsOffset;
            this.flushRequestOffset = flushRequestOffset;
        }

        public long getFlushRequestOffset() {
            return flushRequestOffset;
        }

        @Override
        public String toString() {
            return "Batch{" +
                "opsOffset=" + opsOffset +
                ", flushOffset=" + flushRequestOffset +
                ", size=" + data.size() +
                ", data=" + data +
                '}';
        }

        public void put(Object key, Object value) {
            data.put(key, value);
        }

        public int commit() {
            lock.lock();
            try (lock) {
                int cleaned = Math.toIntExact(opsOffset - getMaxFlushedOpsOffset());
                cleanupFlushedUpdates(opsOffset);
                cleanupFlushRequests();
                return cleaned;
            }
        }

        @Override
        public Iterator<Map.Entry<Object, Object>> iterator() {
            return data.entrySet().iterator();
        }

        public int size() {
            return data.size();
        }
    }

    protected static class UpdateRec {
        protected final long offset;
        protected final Object key;
        protected final Object value;
        protected final long createdAtNanos;

        protected UpdateRec(Object key, Object value, long offset, long createdAtNanos) {
            assert key != null: "key is null";
            assert offset >= 0 : offset;
            this.key = key;
            this.value = value;
            this.offset = offset;
            this.createdAtNanos = createdAtNanos;
        }

        @Override
        public String toString() {
            return "UpdateRec{" +
                    "offset=" + offset +
                    ", key=" + key +
                    ", value=" + value +
                    ", age=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - createdAtNanos) + "ms" +
                    '}';
        }
    }

    protected static class FlushRequestRec {
        protected final long offset;
        protected final long opsOffset;

        protected FlushRequestRec(long opsOffset, long offset) {
            assert opsOffset >= 0: "opsOffset: " + opsOffset;
            assert offset >= 0 : "offset: " + offset;
            this.opsOffset = opsOffset;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return "FlushRequestRec{" +
                    "offset=" + offset +
                    ", opsOffset=" + opsOffset +
                '}';
        }
    }
}
