package com.vladykin.replicamap.kafka.impl;

import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
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

    protected final ArrayDeque<ConsumerRecord<Object,OpMessage>> flushReqs = new ArrayDeque<>();
    protected long maxFlushOffsetOps;
    protected long maxFlushReqOffset;

    public UnprocessedFlushRequests(long maxFlushReqOffset, long maxFlushOffsetOps) {
        this.maxFlushReqOffset = maxFlushReqOffset;
        this.maxFlushOffsetOps = maxFlushOffsetOps;
    }

    @Override
    public String toString() {
        return "UnprocessedFlushRequests{" +
            "flushReqsSize=" + flushReqs.size() +
            ", maxFlushOffsetOps=" + maxFlushOffsetOps +
            ", maxFlushReqOffset=" + maxFlushReqOffset +
            '}';
    }

    public void addFlushRequests(TopicPartition flushPart, List<ConsumerRecord<Object,OpMessage>> partRecs) {
        for (ConsumerRecord<Object,OpMessage> flushReq : partRecs) {
            if (flushReq.offset() <= maxFlushReqOffset) {
                throw new IllegalStateException("Offset of the record must be higher than " + maxFlushReqOffset +
                    " : " + flushReq);
            }

            long flushOffsetOps = flushReq.value().getFlushOffsetOps();

            // We may only add requests to the queue if they are not reordered,
            // otherwise we will not be able to commit the offset out of order.
            if (flushOffsetOps > maxFlushOffsetOps) {
                flushReqs.add(flushReq);
                maxFlushReqOffset = flushReq.offset();
                maxFlushOffsetOps = flushOffsetOps;
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("For partition {} add flush requests, maxFlushReqOffset: {}, maxFlushOffsetOps: {}, flushReqs: {}",
                flushPart, maxFlushReqOffset, maxFlushOffsetOps, flushReqs);
        }
    }

    public OffsetAndMetadata getFlushConsumerOffsetToCommit(long maxOffset) {
        assert !isEmpty();

        long flushConsumerOffset = -1L;

        for (ConsumerRecord<Object,OpMessage> flushReq : flushReqs) {
            if (flushReq.value().getFlushOffsetOps() > maxOffset)
                break;

            flushConsumerOffset = flushReq.offset();
        }

        // We need to commit the offset of the next record, thus + 1.
        return new OffsetAndMetadata(flushConsumerOffset + 1);
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

    public void clearUntil(long maxOffset) {
        int cleared = 0;

        for(;;) {
            ConsumerRecord<Object,OpMessage> flushReq = flushReqs.peek();

            if (flushReq == null || flushReq.value().getFlushOffsetOps() > maxOffset)
                break;

            flushReqs.poll();
            cleared++;
        }

        if (cleared > 0 && log.isDebugEnabled())
            log.debug("Cleared {} flush requests, maxOffset: {}, flushReqs: {}", cleared, maxOffset, flushReqs);
    }

    public boolean isEmpty() {
        return flushReqs.isEmpty();
    }

    public int size() {
        return flushReqs.size();
    }
}
