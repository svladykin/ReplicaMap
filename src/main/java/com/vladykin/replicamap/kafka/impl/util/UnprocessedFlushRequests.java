package com.vladykin.replicamap.kafka.impl.util;

import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import java.util.ArrayDeque;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

/**
 * Queue of unprocessed flush requests and related logic.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class UnprocessedFlushRequests {
    protected final ArrayDeque<ConsumerRecord<Object,OpMessage>> flushReqs = new ArrayDeque<>();
    protected long maxFlushOffsetOps;
    protected long maxFlushReqOffset;

    public UnprocessedFlushRequests(long maxFlushReqOffset, long maxFlushOffsetOps) {
        this.maxFlushReqOffset = maxFlushReqOffset;
        this.maxFlushOffsetOps = maxFlushOffsetOps;
    }

    public void addFlushRequests(List<ConsumerRecord<Object,OpMessage>> partRecs) {
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

    public long getMaxCleanOffsetOps() {
        return flushReqs.stream()
                .mapToLong(rec -> rec.value().getCleanOffsetOps())
                .max().getAsLong();
    }

    public LongStream getFlushOffsetOpsStream() {
        return flushReqs.stream().mapToLong(rec -> rec.value().getFlushOffsetOps());
    }

    public void clearUntil(long maxOffset) {
        for(;;) {
            ConsumerRecord<Object,OpMessage> flushReq = flushReqs.peek();

            if (flushReq == null || flushReq.value().getFlushOffsetOps() > maxOffset)
                break;

            flushReqs.poll();
        }
    }

    public boolean isEmpty() {
        return flushReqs.isEmpty();
    }

    public int size() {
        return flushReqs.size();
    }
}
