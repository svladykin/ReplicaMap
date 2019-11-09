package com.vladykin.replicamap.kafka.impl.worker;

import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.msg.OpMessageSerializer;
import com.vladykin.replicamap.kafka.impl.util.Box;
import com.vladykin.replicamap.kafka.impl.util.FlushQueue;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.vladykin.replicamap.base.ReplicaMapBase.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.base.ReplicaMapBase.OP_FLUSH_REQUEST;
import static com.vladykin.replicamap.base.ReplicaMapBase.OP_PUT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"unchecked", "ArraysAsListWithZeroOrOneArgument", "FieldCanBeLocal", "UnusedReturnValue"})
class OpsWorkerTest {
    static final long CLIENT1_ID = 111;
    static final long CLIENT2_ID = 222;

    static final String TOPIC_DATA = "data";
    static final String TOPIC_OPS = "ops";
    static final String TOPIC_FLUSH = "flush";

    static int FLUSH_MAX_OPS = 10;

    Set<Integer> parts;
    MockConsumer<Object,Object> dataConsumer;
    MockConsumer<Object,OpMessage> opsConsumer;
    MockProducer<Object,OpMessage> flushProducer;
    List<FlushQueue> flushQueues;
    Queue<ConsumerRecord<Object,OpMessage>> cleanQueue;
    OpsWorker opsWorker;
    TopicPartition dataPart;
    TopicPartition opsPart;
    AtomicInteger appliedUpdates;

    @BeforeEach
    void beforeEachTest() {
        appliedUpdates = new AtomicInteger();

        parts = singleton(0);
        opsPart = new TopicPartition(TOPIC_OPS, 0);
        dataPart = new TopicPartition(TOPIC_DATA, 0);

        flushQueues = singletonList(new FlushQueue());
        cleanQueue = new ConcurrentLinkedQueue<>();

        dataConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        opsConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);

        Serializer<?> longSer = new LongSerializer();
        flushProducer = new MockProducer<>(true, (Serializer<Object>)longSer,
            new OpMessageSerializer<>(longSer, null));

        opsConsumer.assign(singleton(opsPart));
        dataConsumer.assign(singleton(dataPart));

        opsWorker = new OpsWorker(
            CLIENT1_ID, TOPIC_DATA, TOPIC_OPS, TOPIC_FLUSH,
            0,
            parts,
            dataConsumer,
            opsConsumer,
            flushProducer,
            FLUSH_MAX_OPS,
            flushQueues,
            cleanQueue,
            this::applyReceivedUpdate
        );
    }

    protected <K, V> boolean applyReceivedUpdate(long clientId, long opId, byte updateType, K key, V exp, V upd,
        BiFunction<?,?,?> function, Box<V> updatedValueBox) {
        appliedUpdates.incrementAndGet();
        return true;
    }

    static ConsumerRecord<Object,OpMessage> newPutRecord(long clientId, long offset) {
        Random rnd = ThreadLocalRandom.current();
        return new ConsumerRecord<>(TOPIC_OPS, 0,
            offset, rnd.nextInt(10), new OpMessage(OP_PUT, clientId, 0, null, rnd.nextInt(10), null));
    }

    protected ConsumerRecord<Object,OpMessage> addPutRecord(long clientId, long offset) {
        ConsumerRecord<Object,OpMessage> rec = newPutRecord(clientId, offset);
        opsConsumer.addRecord(rec);
        return rec;
    }

    static ConsumerRecord<Object,OpMessage> newFlushNotification(long clientId, long flushOffsetData, long flushOffsetOps,  long offset) {
        return new ConsumerRecord<>(
            TOPIC_OPS, 0, offset, null, new OpMessage(OP_FLUSH_NOTIFICATION, clientId, flushOffsetData, flushOffsetOps, 0L));
    }

    protected ConsumerRecord<Object,OpMessage> addFlushNotification(long flushOffsetData, long flushOffsetOps, long offset) {
        ConsumerRecord<Object,OpMessage> rec = newFlushNotification(OpsWorkerTest.CLIENT2_ID, flushOffsetData, flushOffsetOps, offset);
        opsConsumer.addRecord(rec);
        return rec;
    }

    @Test
    void testForwardCompatibility() {
        TopicPartition p = new TopicPartition(TOPIC_OPS, 0);
        opsWorker.applyOpsTopicRecords(p, asList(new ConsumerRecord<>(TOPIC_OPS, p.partition(), 1L, null,
            new OpMessage((byte)'Z', CLIENT1_ID, 100500, null, null, null))));
        opsWorker.applyOpsTopicRecords(p, asList(new ConsumerRecord<>(TOPIC_OPS, p.partition(), 2L, "key",
            new OpMessage((byte)'Z', CLIENT1_ID, 100500, null, null, null))));
    }

    @Test
    void testTryFindLastFlushRecordEmpty() {
        opsConsumer.updateEndOffsets(singletonMap(opsPart, 0L));
        assertSame(OpsWorker.NOT_EXIST,
            opsWorker.tryFindLastFlushRecord(opsPart, 0L, Duration.ofMillis(1)));
    }

    @Test
    void testTryFindLastFlushRecordNotFound() {
        long offset = 1000;

        addFlushNotification(100500, offset - 1, offset++);

        for (int i = 0; i < FLUSH_MAX_OPS; i++)
            addPutRecord(CLIENT1_ID, offset++);

        addFlushNotification(200600, offset - 1, offset++);

        for (int i = 0; i < FLUSH_MAX_OPS; i++)
            addPutRecord(CLIENT1_ID, offset++);

        opsConsumer.updateEndOffsets(singletonMap(opsPart, offset));

        assertSame(OpsWorker.NOT_FOUND, opsWorker.tryFindLastFlushRecord(opsPart,
            opsConsumer.endOffsets(singleton(opsPart)).get(opsPart), Duration.ofMillis(1)));
    }

    @Test
    void testTryFindLastFlushRecord() {
        long offset = 1000;

        addFlushNotification(100500, offset - 1, offset++);

        for (int i = 0; i < FLUSH_MAX_OPS; i++)
            addPutRecord(CLIENT1_ID, offset++);

        ConsumerRecord<Object,OpMessage> rec =
            addFlushNotification(200600, offset - 1, offset++);

        for (int i = 0; i < FLUSH_MAX_OPS - 1; i++)
            addPutRecord(CLIENT1_ID, offset++);

        opsConsumer.updateEndOffsets(singletonMap(opsPart, offset));

        assertSame(rec, opsWorker.tryFindLastFlushRecord(opsPart,
            opsConsumer.endOffsets(singleton(opsPart)).get(opsPart), Duration.ofMillis(1)));
    }

    @Test
    void testFindLastFlushRecordEmpty() {
        opsConsumer.updateEndOffsets(singletonMap(opsPart, 0L));
        assertSame(null,
            opsWorker.findLastFlushRecord(dataPart, opsPart, Utils.millis(1)));
    }

    @Test
    void testFindLastFlushRecord() {
        Runnable callback = () -> {
            long offset = 1000;

            addFlushNotification(100500, offset - 1, offset++);

            for (int i = 0; i < FLUSH_MAX_OPS; i++)
                addPutRecord(CLIENT1_ID, offset++);

            addFlushNotification(200600, offset - 1, offset++);

            for (int i = 0; i < 3 * FLUSH_MAX_OPS; i++)
                addPutRecord(CLIENT1_ID, offset++);

            addFlushNotification(300700, offset - 1, offset++);

            opsConsumer.updateEndOffsets(singletonMap(opsPart, offset));
            dataConsumer.updateEndOffsets(singletonMap(dataPart, 300700L));
        };

        callback.run();
        for (int i = 0; i < 4; i++)
            opsConsumer.schedulePollTask(callback);

        assertEquals(200600L, opsWorker.findLastFlushRecord(dataPart, opsPart, Utils.millis(1))
            .value().getFlushOffsetData());
    }

    @Test
    void testSeekOpsOffsets() {
        opsWorker.seekOpsOffsets(singletonMap(opsPart, 111511L));
        assertEquals(111511L, opsConsumer.position(opsPart));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void testApplyOpsTopicRecords() {
        opsWorker.applyOpsTopicRecords(opsPart, asList(
            newPutRecord(CLIENT1_ID, 1001),
            newPutRecord(CLIENT2_ID, 1002),
            newPutRecord(CLIENT1_ID, 1003),
            newFlushNotification(CLIENT1_ID, 100500, 1003, 1004),
            newPutRecord(CLIENT1_ID, 1005),
            newPutRecord(CLIENT2_ID, 1006),
            newPutRecord(CLIENT1_ID, 1007),
            newFlushNotification(CLIENT2_ID, 200700, 1007, 1008),
            newPutRecord(CLIENT1_ID, 1009),
            newPutRecord(CLIENT1_ID, 1010)
        ));

        assertEquals(8, appliedUpdates.get());
        assertEquals(1, cleanQueue.size());
        assertEquals(200700, cleanQueue.poll().value().getFlushOffsetData());

        assertEquals(1, flushProducer.history().size());
        OpMessage flushReq = flushProducer.history().get(0).value();
        assertEquals(1010, flushReq.getFlushOffsetOps());

        assertEquals(200700, opsWorker.lastFlushNotifications.get(opsPart).getFlushOffsetData());
    }

    @Test
    void testLoadDataForPartition() {
        long offset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, offset));
        dataConsumer.addRecord(new ConsumerRecord<>(TOPIC_DATA, 0, ++offset, "a", "A"));
        dataConsumer.addRecord(new ConsumerRecord<>(TOPIC_DATA, 0, ++offset, "b", "B"));
        dataConsumer.addRecord(new ConsumerRecord<>(TOPIC_DATA, 0, ++offset, "c", "C"));
        dataConsumer.addRecord(new ConsumerRecord<>(TOPIC_DATA, 0, ++offset, "d", "D"));

        assertEquals(3, opsWorker.loadDataForPartition(dataPart, offset - 1, Duration.ofMillis(0)));
        assertEquals(3, appliedUpdates.get());
    }

    @Test
    void testIsActuallySteady() {
        long endOffset = 100L;
        opsConsumer.updateEndOffsets(singletonMap(opsPart, endOffset));

        assertNull(opsWorker.endOffsetsOps);
        assertEquals(0L, opsWorker.maxAllowedSteadyLag);

        opsConsumer.seek(opsPart, endOffset - FLUSH_MAX_OPS - 1);
        assertFalse(opsWorker.isActuallySteady());

        assertNotNull(opsWorker.endOffsetsOps);
        assertEquals(0L, opsWorker.maxAllowedSteadyLag);

        opsConsumer.seek(opsPart, endOffset);
        opsConsumer.updateEndOffsets(singletonMap(opsPart, endOffset + FLUSH_MAX_OPS + 1));
        assertFalse(opsWorker.isActuallySteady());

        assertNotNull(opsWorker.endOffsetsOps);
        assertEquals(FLUSH_MAX_OPS, opsWorker.maxAllowedSteadyLag);

        assertFalse(opsWorker.isActuallySteady());

        assertNotNull(opsWorker.endOffsetsOps);
        assertEquals(FLUSH_MAX_OPS, opsWorker.maxAllowedSteadyLag);

        opsConsumer.seek(opsPart, endOffset + 1);
        assertTrue(opsWorker.isActuallySteady());

        assertNull(opsWorker.endOffsetsOps);
        assertEquals(FLUSH_MAX_OPS, opsWorker.maxAllowedSteadyLag);
    }

    @Test
    void testProcessOpsRecords() {
        long endOffset = 100L;
        opsConsumer.updateEndOffsets(singletonMap(opsPart, endOffset));
        opsConsumer.seek(opsPart, endOffset - FLUSH_MAX_OPS - 1);

        CompletableFuture<Void> steadyFut = opsWorker.getSteadyFuture();

        assertFalse(steadyFut.isDone());
        assertFalse(opsWorker.processOpsRecords(new ConsumerRecords<>(singletonMap(opsPart, asList(
            newPutRecord(CLIENT2_ID, 50),
            newPutRecord(CLIENT2_ID, 51)
        )))));

        assertEquals(2, appliedUpdates.get());
        assertFalse(steadyFut.isDone());

        assertFalse(opsWorker.processOpsRecords(new ConsumerRecords<>(emptyMap())));
        assertFalse(steadyFut.isDone());

        opsConsumer.seek(opsPart, endOffset);

        assertTrue(opsWorker.processOpsRecords(new ConsumerRecords<>(emptyMap())));
        assertTrue(steadyFut.isDone());

        assertFalse(opsWorker.processOpsRecords(new ConsumerRecords<>(emptyMap())));

        assertFalse(opsWorker.processOpsRecords(new ConsumerRecords<>(singletonMap(opsPart, asList(
            newPutRecord(CLIENT1_ID, 70),
            newPutRecord(CLIENT2_ID, 71)
        )))));

        assertEquals(4, appliedUpdates.get());

        List<ProducerRecord<Object,OpMessage>> flush = flushProducer.history();
        assertEquals(1, flush.size());

        ProducerRecord<Object,OpMessage> prepFlushReqRec = flush.get(0);
        assertNull(prepFlushReqRec.key());

        OpMessage flushReqOp = prepFlushReqRec.value();
        assertEquals(OP_FLUSH_REQUEST, flushReqOp.getOpType());
        assertEquals(CLIENT1_ID, flushReqOp.getClientId());
        assertEquals(70, flushReqOp.getFlushOffsetOps());
    }

    @Test
    void testLoadData() {
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, 100L));
        dataConsumer.addRecord(new ConsumerRecord<>(TOPIC_DATA, 0, 100L, "a", "A"));
        dataConsumer.addRecord(new ConsumerRecord<>(TOPIC_DATA, 0, 101L, "b", "B"));
        dataConsumer.addRecord(new ConsumerRecord<>(TOPIC_DATA, 0, 102L, "c", "C"));
        dataConsumer.addRecord(new ConsumerRecord<>(TOPIC_DATA, 0, 103L, "d", "D"));
        dataConsumer.updateEndOffsets(singletonMap(dataPart, 104L));

        addPutRecord(CLIENT2_ID, 1000L);
        addPutRecord(CLIENT2_ID, 1001L);
        addFlushNotification(102L, 1000L, 1002L);
        addPutRecord(CLIENT2_ID, 1003L);

        opsConsumer.updateEndOffsets(singletonMap(opsPart, 1004L));

        assertEquals(singletonMap(opsPart, 1001L), opsWorker.loadData());
        assertEquals(102L, opsWorker.lastFlushNotifications.get(opsPart).getFlushOffsetData());
        assertEquals(3, appliedUpdates.get());
    }
}