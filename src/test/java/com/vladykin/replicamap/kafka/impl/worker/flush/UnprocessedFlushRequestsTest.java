package com.vladykin.replicamap.kafka.impl.worker.flush;

import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnprocessedFlushRequestsTest {
    @Test
    void testSimple() {
        TopicPartition flushPart = new TopicPartition("bla", 6);
        UnprocessedFlushRequests reqs = new UnprocessedFlushRequests(flushPart, 10, 20);

        assertEquals(flushPart, reqs.flushPart);

        assertTrue(reqs.isEmpty());
        assertEquals(0, reqs.size());
        assertEquals(10, reqs.maxFlushReqOffset);
        assertEquals(20, reqs.maxFlushOffsetOps);
        assertArrayEquals(new long[]{}, reqs.getFlushOffsetOpsStream().toArray());

        assertThrows(IllegalStateException.class, () ->
            reqs.addFlushRequests(asList(new ConsumerRecord<>("flush", 0, 10, null,
                new FlushRequest(0, 21, 0)))));

        assertTrue(reqs.isEmpty());
        assertEquals(0, reqs.size());
        assertEquals(10, reqs.maxFlushReqOffset);
        assertEquals(20, reqs.maxFlushOffsetOps);
        assertArrayEquals(new long[]{}, reqs.getFlushOffsetOpsStream().toArray());

        reqs.addFlushRequests(asList(new ConsumerRecord<>("flush", 0, 11, null,
            new FlushRequest(0, 20, 0))));

        assertTrue(reqs.isEmpty());
        assertEquals(0, reqs.size());
        assertEquals(10, reqs.maxFlushReqOffset);
        assertEquals(20, reqs.maxFlushOffsetOps);
        assertArrayEquals(new long[]{}, reqs.getFlushOffsetOpsStream().toArray());

        reqs.addFlushRequests(asList(new ConsumerRecord<>("flush", 0, 11, null,
            new FlushRequest(0, 21, 7))));

        assertFalse(reqs.isEmpty());
        assertEquals(1, reqs.size());
        assertEquals(11, reqs.maxFlushReqOffset);
        assertEquals(21, reqs.maxFlushOffsetOps);
        assertArrayEquals(new long[]{21}, reqs.getFlushOffsetOpsStream().toArray());
        assertEquals(7, reqs.getMaxCleanOffsetOps());

        reqs.addFlushRequests(asList(new ConsumerRecord<>("flush", 0, 12, null,
            new FlushRequest(0, 21, 19))));

        assertFalse(reqs.isEmpty());
        assertEquals(1, reqs.size());
        assertEquals(11, reqs.maxFlushReqOffset);
        assertEquals(21, reqs.maxFlushOffsetOps);
        assertArrayEquals(new long[]{21}, reqs.getFlushOffsetOpsStream().toArray());
        assertEquals(7, reqs.getMaxCleanOffsetOps());

        reqs.addFlushRequests(asList(new ConsumerRecord<>("flush", 0, 13, null,
            new FlushRequest(0, 23, 9))));

        assertFalse(reqs.isEmpty());
        assertEquals(2, reqs.size());
        assertEquals(13, reqs.maxFlushReqOffset);
        assertEquals(23, reqs.maxFlushOffsetOps);
        assertArrayEquals(new long[]{21, 23}, reqs.getFlushOffsetOpsStream().toArray());
        assertEquals(9, reqs.getMaxCleanOffsetOps());

        reqs.addFlushRequests(asList(new ConsumerRecord<>("flush", 0, 15, null,
            new FlushRequest(0, 25, 8))));

        assertFalse(reqs.isEmpty());
        assertEquals(3, reqs.size());
        assertEquals(15, reqs.maxFlushReqOffset);
        assertEquals(25, reqs.maxFlushOffsetOps);
        assertArrayEquals(new long[]{21, 23, 25}, reqs.getFlushOffsetOpsStream().toArray());
        assertEquals(9, reqs.getMaxCleanOffsetOps());

        assertThrows(IllegalStateException.class, () -> reqs.getFlushConsumerOffsetToCommit(20));
        assertThrows(IllegalStateException.class, () -> reqs.getFlushConsumerOffsetToCommit(22));
        assertThrows(IllegalStateException.class, () -> reqs.getFlushConsumerOffsetToCommit(24));
        assertThrows(IllegalStateException.class, () -> reqs.getFlushConsumerOffsetToCommit(26));

        assertEquals(12, reqs.getFlushConsumerOffsetToCommit(21).offset());
        assertEquals(14, reqs.getFlushConsumerOffsetToCommit(23).offset());
        assertEquals(16, reqs.getFlushConsumerOffsetToCommit(25).offset());

        reqs.clearUntil(20);

        assertFalse(reqs.isEmpty());
        assertEquals(3, reqs.size());
        assertEquals(15, reqs.maxFlushReqOffset);
        assertEquals(25, reqs.maxFlushOffsetOps);
        assertArrayEquals(new long[]{21, 23, 25}, reqs.getFlushOffsetOpsStream().toArray());
        assertEquals(9, reqs.getMaxCleanOffsetOps());

        reqs.clearUntil(21);

        assertFalse(reqs.isEmpty());
        assertEquals(2, reqs.size());
        assertEquals(15, reqs.maxFlushReqOffset);
        assertEquals(25, reqs.maxFlushOffsetOps);
        assertArrayEquals(new long[]{23, 25}, reqs.getFlushOffsetOpsStream().toArray());
        assertEquals(9, reqs.getMaxCleanOffsetOps());

        reqs.clearUntil(22);

        assertFalse(reqs.isEmpty());
        assertEquals(2, reqs.size());
        assertEquals(15, reqs.maxFlushReqOffset);
        assertEquals(25, reqs.maxFlushOffsetOps);
        assertArrayEquals(new long[]{23, 25}, reqs.getFlushOffsetOpsStream().toArray());
        assertEquals(9, reqs.getMaxCleanOffsetOps());

        reqs.clearUntil(25);

        assertTrue(reqs.isEmpty());
        assertEquals(0, reqs.size());
        assertEquals(15, reqs.maxFlushReqOffset);
        assertEquals(25, reqs.maxFlushOffsetOps);
        assertArrayEquals(new long[]{}, reqs.getFlushOffsetOpsStream().toArray());
    }
}