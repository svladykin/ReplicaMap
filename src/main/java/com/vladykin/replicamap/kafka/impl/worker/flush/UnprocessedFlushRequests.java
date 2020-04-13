package com.vladykin.replicamap.kafka.impl.worker.flush;

import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import java.util.ArrayDeque;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queue of unprocessed flush requests and related logic.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class UnprocessedFlushRequests {
    private static final Logger log = LoggerFactory.getLogger(UnprocessedFlushRequests.class);

    protected final TopicPartition flushPart;
    protected final ArrayDeque<ConsumerRecord<Object,FlushRequest>> flushReqs = new ArrayDeque<>();
    protected long maxFlushOffsetOps = -1;
    protected long maxFlushReqOffset;
    protected boolean initialized;

    public UnprocessedFlushRequests(TopicPartition flushPart, long firstRecOffset, boolean initialized) {
        this.flushPart = flushPart;
        this.initialized = initialized;

        if (initialized) {
            assert firstRecOffset == 0: firstRecOffset;
            this.maxFlushReqOffset = -1;
        }
        else // Current max will be before the previous record of the first one.
            this.maxFlushReqOffset = firstRecOffset - 2;
    }

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public String toString() {
        return "UnprocessedFlushRequests{" +
            "flushReqsSize=" + flushReqs.size() +
            ", maxFlushOffsetOps=" + maxFlushOffsetOps +
            ", maxFlushReqOffset=" + maxFlushReqOffset +
            '}';
    }

    public void addFlushRequests(List<ConsumerRecord<Object,FlushRequest>> partRecs) {
        for (ConsumerRecord<Object,FlushRequest> flushReq : partRecs) {
            // Check that we do not miss records.
            if (flushReq.offset() != 1 + maxFlushReqOffset) {
                throw new IllegalStateException("Invalid offset, expected: " + maxFlushReqOffset +
                    ", actual: " + flushReq);
            }
            maxFlushReqOffset++;

            long flushOffsetOps = flushReq.value().getFlushOffsetOps();

            if (!initialized) {
                initialized = true;
                maxFlushOffsetOps = flushOffsetOps;
                continue; // It was the last previously committed flush request.
            }

            // We may only add requests to the queue if they are not reordered,
            // otherwise we will not be able to commit the offset out of order.
            if (flushOffsetOps > maxFlushOffsetOps) {
                flushReqs.add(flushReq);
                maxFlushOffsetOps = flushOffsetOps;
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("For partition {} add flush requests, maxFlushReqOffset: {}, maxFlushOffsetOps: {}, flushReqs: {}",
                flushPart, maxFlushReqOffset, maxFlushOffsetOps, flushReqs);
        }
    }

    public OffsetAndMetadata getFlushConsumerOffsetToCommit(long flushOffsetOps) {
        assert !isEmpty();

        for (ConsumerRecord<Object,FlushRequest> flushReq : flushReqs) {
            if (flushReq.value().getFlushOffsetOps() == flushOffsetOps)
                return new OffsetAndMetadata(flushReq.offset() + 1); // We need to commit the offset of the next record, thus + 1.
        }

        throw new IllegalStateException("Failed to find flush request with ops offset " + flushOffsetOps);
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public long getMaxCleanOffsetOps() {
        return flushReqs.stream()
                .mapToLong(rec -> rec.value().getCleanOffsetOps())
                .max().getAsLong();
    }

    public LongStream getFlushOffsetOpsStream() {
        return flushReqs.stream().mapToLong(rec -> rec.value().getFlushOffsetOps());
    }

    public void clearUntil(long maxFlushOffsetOps) {
        int cleared = 0;

        for(;;) {
            ConsumerRecord<Object,FlushRequest> flushReq = flushReqs.peek();

            if (flushReq == null || flushReq.value().getFlushOffsetOps() > maxFlushOffsetOps)
                break;

            flushReqs.poll();
            cleared++;
        }

        if (log.isDebugEnabled()) {
            log.debug("Cleared for partition {} {} flush requests, maxOffset: {}, flushReqs: {}",
                flushPart, cleared, maxFlushOffsetOps, flushReqs);
        }
    }

    public boolean isEmpty() {
        return flushReqs.isEmpty();
    }

    public int size() {
        return flushReqs.size();
    }
}
