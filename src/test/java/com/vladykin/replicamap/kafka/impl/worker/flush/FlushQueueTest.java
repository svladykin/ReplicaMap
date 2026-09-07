package com.vladykin.replicamap.kafka.impl.worker.flush;

import com.vladykin.replicamap.kafka.impl.msg.FlushNotification;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.vladykin.replicamap.kafka.impl.worker.flush.FlushQueue.NOT_INITIALIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlushQueueTest {

    private static final String DATA_TOPIC = "data_topic";
    private static final String FLUSH_TOPIC = "flush_topic";
    private static final int PART = 5;
    private static final UUID CLIENT_ID = new UUID(0,0);

    @Test
    void testInitUnflushedOpsOffsetResumeArming() {
        // Armed: the resume flush notification must be replayed before any later one, and by steady state.
        var q = new FlushQueue(new TopicPartition(DATA_TOPIC, PART));
        q.initUnflushedOpsOffset(99, 50);

        assertThrows(IllegalStateException.class, q::checkFoundResumeFlushNotification);
        assertThrows(IllegalStateException.class,
            () -> q.addOpsRecord(null, null, 100, false, new FlushNotification(CLIENT_ID, 60, 0)));

        // Armed, happy path: the resume notification is replayed first, later ones are fine.
        var q2 = new FlushQueue(new TopicPartition(DATA_TOPIC, PART));
        q2.initUnflushedOpsOffset(99, 50);

        q2.addOpsRecord(null, null, 100, false, new FlushNotification(CLIENT_ID, 50, 0));
        q2.addOpsRecord(null, null, 101, false, new FlushNotification(CLIENT_ID, 60, 0));
        q2.checkFoundResumeFlushNotification();

        // Disarmed (-1): the resume notification is known to not be replayed (verified skip of a
        // retention-deleted range) — later notifications and the steady check must pass.
        var q3 = new FlushQueue(new TopicPartition(DATA_TOPIC, PART));
        q3.initUnflushedOpsOffset(99, -1);

        q3.checkFoundResumeFlushNotification();
        q3.addOpsRecord(null, null, 100, false, new FlushNotification(CLIENT_ID, 60, 0));
        q3.checkFoundResumeFlushNotification();
    }

    @Test
    void testStaleFlushOffsetReArm() {
        var q = new FlushQueue(new TopicPartition(DATA_TOPIC, PART));
        q.initUnflushedOpsOffset(99);

        long past = System.nanoTime() - TimeUnit.SECONDS.toNanos(10);
        q.addOpsRecord("a", "A", 100, true, null, past);
        q.addOpsRecord("b", "B", 101, true, null, past);

        long maxAge = TimeUnit.MILLISECONDS.toNanos(500);

        // Tail younger than the threshold -> no flush due.
        assertEquals(-1, q.staleFlushOffset(TimeUnit.HOURS.toNanos(1)));

        // Stale tail is returned once, then de-spammed.
        assertEquals(101, q.staleFlushOffset(maxAge));
        assertEquals(-1, q.staleFlushOffset(maxAge));

        // Failed send for an outdated offset must not re-arm.
        q.onStaleFlushRequestFailed(100);
        assertEquals(-1, q.staleFlushOffset(maxAge));

        // Failed send for the requested offset re-arms the trigger.
        q.onStaleFlushRequestFailed(101);
        assertEquals(101, q.staleFlushOffset(maxAge));
        assertEquals(-1, q.staleFlushOffset(maxAge));

        // A newer tail re-arms it naturally.
        q.addOpsRecord("c", "C", 102, true, null, past);
        assertEquals(102, q.staleFlushOffset(maxAge));
    }

    @Test
    void testSimple() {
        var dataPart = new TopicPartition(DATA_TOPIC, PART);
        var q = new FlushQueue(dataPart);

        assertEquals(dataPart, q.getDataPartition());
        assertEquals(NOT_INITIALIZED, q.maxAddedOpsOffset);

        q.initUnflushedOpsOffset(-1);

        assertEquals(-1, q.getMaxFlushedOpsOffset());
        assertEquals(-1, q.maxAddedOpsOffset);
        assertEquals(0, q.unflushedUpdates.size());

        assertEquals(-1, q.addOpsRecord(1,"v", 0, true, null));

        assertEquals(-1, q.getMaxFlushedOpsOffset());
        assertEquals(0, q.maxAddedOpsOffset);
        assertEquals(1, q.unflushedUpdates.size());

        long flushOffset = -1;

        // Empty flush requests -> noop.
        var batch = q.collectBatch(newFlushRequests(++flushOffset));
        assertNull(batch);
        assertTrue(q.flushRequests.isEmpty());
        assertEquals(-1, q.getMaxFlushedOpsOffset());
        assertEquals(0, q.maxAddedOpsOffset);
        assertEquals(1, q.unflushedUpdates.size());

        // Too high flush request -> no batch, but saved the flush request for the future.
        batch = q.collectBatch(newFlushRequests(++flushOffset, 1));
        assertNull(batch);
        assertEquals(1, q.flushRequests.size());
        assertEquals(flushOffset, q.flushRequests.getLast().offset);
        assertEquals(1, q.flushRequests.getLast().opsOffset);
        assertEquals(-1, q.getMaxFlushedOpsOffset());
        assertEquals(0, q.maxAddedOpsOffset);
        assertEquals(1, q.unflushedUpdates.size());

        // Too high duplicate flush request -> no batch, but saved the flush request for the future with the new offset.
        batch = q.collectBatch(newFlushRequests(++flushOffset, 1));
        assertNull(batch);
        assertEquals(1, q.flushRequests.size());
        assertEquals(flushOffset, q.flushRequests.getLast().offset);
        assertEquals(1, q.flushRequests.getLast().opsOffset);
        assertEquals(-1, q.getMaxFlushedOpsOffset());
        assertEquals(0, q.maxAddedOpsOffset);
        assertEquals(1, q.unflushedUpdates.size());

        // Unsuccessful update -> will cause flush but the value is old.
        assertEquals(0, q.addOpsRecord(1,"z", 1, false, null));
        batch = q.collectBatch(newFlushRequests(++flushOffset, 1));
        assertEquals(1, batch.size());
        assertEquals(1, q.flushRequests.size());
        assertEquals(flushOffset, q.flushRequests.getLast().offset);
        assertEquals(1, q.flushRequests.getLast().opsOffset);
        assertEquals(-1, q.getMaxFlushedOpsOffset());
        assertEquals(1, q.maxAddedOpsOffset);
        assertEquals(1, q.unflushedUpdates.size());

        assertEquals(2, batch.commit());
        assertEquals(0, q.flushRequests.size());
        assertEquals(1, q.getMaxFlushedOpsOffset());
        assertEquals(1, q.maxAddedOpsOffset);
        assertEquals(0, q.unflushedUpdates.size());
    }

    private List<ConsumerRecord<Object, FlushRequest>> newFlushRequests(long startFlushOffset, long... opsOffsets) {
        assertTrue(startFlushOffset >= 0);
        var list = new ArrayList<ConsumerRecord<Object, FlushRequest>>();
        for (long opsOffset : opsOffsets) {
            assertTrue(opsOffset >= 0);
            list.add(new ConsumerRecord<>(FLUSH_TOPIC, PART, startFlushOffset++, null, new FlushRequest(CLIENT_ID, opsOffset)));
        }
        return list;
    }

    @Test
    void testBatch() {
        var dataPart = new TopicPartition(DATA_TOPIC, PART);
        var q = new FlushQueue(dataPart);

        assertEquals(dataPart, q.getDataPartition());
        assertEquals(NOT_INITIALIZED, q.maxAddedOpsOffset);

        q.initUnflushedOpsOffset(-1);

        assertEquals(-1, q.getMaxFlushedOpsOffset());
        assertEquals(-1, q.maxAddedOpsOffset);
        assertEquals(0, q.unflushedUpdates.size());

        assertEquals(-1, q.addOpsRecord("A","v", 10, true, null));
        assertEquals(10, q.addOpsRecord("A","V", 11, false, null));
        assertEquals(11, q.addOpsRecord("B","b", 12, true, null));

        assertEquals(9, q.getMaxFlushedOpsOffset());
        assertEquals(12, q.maxAddedOpsOffset);
        assertEquals(2, q.unflushedUpdates.size());

        assertEquals(12, q.addOpsRecord("C","c", 14, true, null));
        assertEquals(14, q.addOpsRecord("D","d", 17, true, null));
        assertEquals(17, q.addOpsRecord("D",null, 19, true, null));

        assertEquals(9, q.getMaxFlushedOpsOffset());
        assertEquals(19, q.maxAddedOpsOffset);
        assertEquals(5, q.unflushedUpdates.size());

        long flushOffset = 1000;

        var batch = q.collectBatch(newFlushRequests(++flushOffset, 9));
        assertNull(batch);
        assertTrue(q.flushRequests.isEmpty());

        batch = q.collectBatch(newFlushRequests(++flushOffset, 10));
        assertEquals(1, batch.size());
        assertEquals(Map.of("A", "v"), batch.data);
        assertEquals(1, q.flushRequests.size());

        batch = q.collectBatch(newFlushRequests(++flushOffset, 11));
        assertEquals(1, batch.size());
        assertEquals(Map.of("A", "v"), batch.data);
        assertEquals(2, q.flushRequests.size());

        batch = q.collectBatch(newFlushRequests(++flushOffset, 12));
        assertEquals(2, batch.size());
        assertEquals(Map.of("A", "v", "B", "b"), batch.data);
        assertEquals(3, q.flushRequests.size());

        batch = q.collectBatch(newFlushRequests(++flushOffset, 13));
        assertEquals(2, batch.size());
        assertEquals(Map.of("A", "v", "B", "b"), batch.data);
        assertEquals(4, q.flushRequests.size());

        batch = q.collectBatch(newFlushRequests(++flushOffset, 14));
        assertEquals(3, batch.size());
        assertEquals(Map.of("A", "v", "B", "b", "C", "c"), batch.data);
        assertEquals(5, q.flushRequests.size());

        batch = q.collectBatch(newFlushRequests(++flushOffset, 17));
        assertEquals(4, batch.size());
        assertEquals(Map.of("A", "v", "B", "b", "C", "c", "D", "d"), batch.data);
        assertEquals(6, q.flushRequests.size());

        batch = q.collectBatch(newFlushRequests(++flushOffset, 18));
        assertEquals(4, batch.size());
        assertEquals(Map.of("A", "v", "B", "b", "C", "c", "D", "d"), batch.data);
        assertEquals(7, q.flushRequests.size());

        batch = q.collectBatch(newFlushRequests(++flushOffset, 19));
        assertEquals(4, batch.size());
        var map = new HashMap<>(Map.of("A", "v", "B", "b", "C", "c"));
        map.put("D", null); // Remove op.
        assertEquals(map, batch.data);
        assertEquals(8, q.flushRequests.size());

        batch = q.collectBatch(newFlushRequests(++flushOffset, 20));
        assertEquals(4, batch.size());
        map = new HashMap<>(Map.of("A", "v", "B", "b", "C", "c"));
        map.put("D", null); // Remove op.
        assertEquals(map, batch.data);
        assertEquals(9, q.flushRequests.size());
    }

    @Test
    void testFlushNotification() {
        var dataPart = new TopicPartition(DATA_TOPIC, PART);
        var q = new FlushQueue(dataPart);

        assertEquals(dataPart, q.getDataPartition());
        assertEquals(NOT_INITIALIZED, q.maxAddedOpsOffset);

        q.initUnflushedOpsOffset(-1);

        assertEquals(-1, q.getMaxFlushedOpsOffset());
        assertEquals(-1, q.maxAddedOpsOffset);
        assertEquals(0, q.unflushedUpdates.size());

        assertEquals(-1, q.addOpsRecord(1,"v", 0, true, null));
        assertEquals(0, q.addOpsRecord(1,"V", 1, false, null));
        assertEquals(1, q.addOpsRecord(2,"x", 2, true, null));

        assertEquals(-1, q.getMaxFlushedOpsOffset());
        assertEquals(2, q.maxAddedOpsOffset);
        assertEquals(2, q.unflushedUpdates.size());

        assertEquals(2, q.addOpsRecord(null, null, 3, false,
                new FlushNotification(CLIENT_ID, 2, 0)));

        assertEquals(3, q.getMaxFlushedOpsOffset());
        assertEquals(3, q.maxAddedOpsOffset);
        assertEquals(0, q.unflushedUpdates.size());

        assertEquals(3, q.addOpsRecord(1,"A", 10, true, null));
        assertEquals(10, q.addOpsRecord(1,"B", 15, true, null));
        assertEquals(15, q.addOpsRecord(2,"X", 20, true, null));

        assertEquals(9, q.getMaxFlushedOpsOffset());
        assertEquals(20, q.maxAddedOpsOffset);
        assertEquals(3, q.unflushedUpdates.size());

        assertEquals(20, q.addOpsRecord(null, null, 21, false,
                new FlushNotification(CLIENT_ID, 15, 0)));

        assertEquals(19, q.getMaxFlushedOpsOffset());
        assertEquals(21, q.maxAddedOpsOffset);
        assertEquals(1, q.unflushedUpdates.size());

        assertEquals(21, q.addOpsRecord(null, null, 22, false,
                new FlushNotification(CLIENT_ID, 19, 0)));

        assertEquals(19, q.getMaxFlushedOpsOffset());
        assertEquals(22, q.maxAddedOpsOffset);
        assertEquals(1, q.unflushedUpdates.size());

        assertEquals(22, q.addOpsRecord(null, null, 23, false,
                new FlushNotification(CLIENT_ID, 21, 0)));

        assertEquals(23, q.getMaxFlushedOpsOffset());
        assertEquals(23, q.maxAddedOpsOffset);
        assertEquals(0, q.unflushedUpdates.size());
    }

    @Test
    void testChecks() {
        FlushQueue q = new FlushQueue(null);

        assertThrows(IllegalStateException.class, () -> q.addOpsRecord("key", "val", 7, true, null));
        assertThrows(IllegalArgumentException.class, () -> q.initUnflushedOpsOffset(-2));
        q.initUnflushedOpsOffset(-1);
        assertThrows(IllegalStateException.class, () -> q.initUnflushedOpsOffset(5));
    }
}