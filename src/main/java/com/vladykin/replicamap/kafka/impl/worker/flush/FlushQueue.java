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

    protected final ArrayDeque<FlushRequestRec> flushRequests = new ArrayDeque<>();

    public FlushQueue(TopicPartition dataPart) {
        this.dataPart = dataPart;
    }

    public TopicPartition getDataPartition() {
        return dataPart;
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

                // Cleanup all the flushed updates from the queue.
                // Here we have all the flushed updates because we get the notification from the same ops partition,
                // e.g. all the needed updates are guaranteed to reside in the log before that notification gets into the log.
                cleanupFlushedUpdates(flushNotification.getOpsOffset());
            } else if (successfulUpdate) {
                var rec = new UpdateRec(key, value, offset);
                log.trace("For partition {} adding update record: {}", dataPart, rec);
                unflushedUpdates.addLast(rec);
            }
            cleanupFlushRequests();
            return oldMaxUnflushedOpsOffset;
        }
    }

    protected long getMaxFlushedOpsOffset() {
        assert maxAddedOpsOffset != NOT_INITIALIZED;
        return unflushedUpdates.isEmpty() ? maxAddedOpsOffset : (unflushedUpdates.peek().offset - 1);
    }

    /**
     * Set up the unflushed ops offset before adding records.
     *
     * @param offset Offset.
     */
    public void initUnflushedOpsOffset(long offset) {
        if (offset <= NOT_INITIALIZED)
            throw new IllegalArgumentException("Illegal offset: " + offset);

        lock.lock();
        try (lock) {
            if (maxAddedOpsOffset != NOT_INITIALIZED)
                throw new IllegalStateException("Max offset is already set: " + maxAddedOpsOffset);

            maxAddedOpsOffset = offset;
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

        protected UpdateRec(Object key, Object value, long offset) {
            assert key != null: "key is null";
            assert offset >= 0 : offset;
            this.key = key;
            this.value = value;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return "UpdateRec{" +
                    "offset=" + offset +
                    ", key=" + key +
                    ", value=" + value +
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
