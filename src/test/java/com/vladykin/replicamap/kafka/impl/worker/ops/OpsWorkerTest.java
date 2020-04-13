package com.vladykin.replicamap.kafka.impl.worker.ops;

import com.vladykin.replicamap.kafka.impl.msg.FlushNotification;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import com.vladykin.replicamap.kafka.impl.msg.MapUpdate;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.msg.OpMessageSerializer;
import com.vladykin.replicamap.kafka.impl.util.Box;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import com.vladykin.replicamap.kafka.impl.worker.flush.FlushQueue;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
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

import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_REQUEST;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_PUT;
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
public class OpsWorkerTest {
    public static final long CLIENT1_ID = 111;
    public static final long CLIENT2_ID = 222;

    public static final String TOPIC_DATA = "data";
    public static final String TOPIC_OPS = "ops";
    public static final String TOPIC_FLUSH = "flush";

    static int FLUSH_MAX_OPS = 10;

    Set<Integer> parts;
    MockConsumer<Object,Object> dataConsumer;
    MockConsumer<Object,OpMessage> opsConsumer;
    MockProducer<Object,FlushRequest> flushProducer;
    List<FlushQueue> flushQueues;
    Queue<ConsumerRecord<Object,FlushNotification>> cleanQueue;
    OpsWorker opsWorker;
    TopicPartition dataPart;
    TopicPartition opsPart;
    AtomicInteger appliedUpdates;

    @SuppressWarnings("rawtypes")
    @BeforeEach
    void beforeEachTest() {
        appliedUpdates = new AtomicInteger();

        parts = singleton(0);
        opsPart = new TopicPartition(TOPIC_OPS, 0);
        dataPart = new TopicPartition(TOPIC_DATA, 0);

        flushQueues = singletonList(new FlushQueue(null));
        cleanQueue = new ConcurrentLinkedQueue<>();

        dataConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        opsConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);

        Serializer<?> longSer = new LongSerializer();
        flushProducer = new MockProducer(true, longSer,
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
            this::applyReceivedUpdate,
            new LongAdder(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder()
        );
    }

    protected <K, V> boolean applyReceivedUpdate(String topic, int part, long offset,
        long clientId, long opId, byte updateType, K key, V exp, V upd,
        BiFunction<?,?,?> function, Box<V> updatedValueBox) {
        appliedUpdates.incrementAndGet();
        return true;
    }

    static ConsumerRecord<Object,OpMessage> newPutRecord(long clientId, long offset) {
        Random rnd = ThreadLocalRandom.current();
        return new ConsumerRecord<>(TOPIC_OPS, 0,
            offset, rnd.nextInt(10), new MapUpdate(OP_PUT, clientId, 0, null, rnd.nextInt(10), null));
    }

    protected ConsumerRecord<Object,OpMessage> addPutRecord(long clientId, long offset) {
        ConsumerRecord<Object,OpMessage> rec = newPutRecord(clientId, offset);
        opsConsumer.addRecord(rec);
        return rec;
    }

    public static ConsumerRecord<Object,FlushNotification> newFlushNotification(long clientId, long flushOffsetData, long flushOffsetOps,  long offset) {
        return new ConsumerRecord<>(
            TOPIC_OPS, 0, offset, null, new FlushNotification(clientId, flushOffsetData, flushOffsetOps));
    }

    protected ConsumerRecord<Object,FlushNotification> addFlushNotification(long flushOffsetData, long flushOffsetOps, long offset) {
        ConsumerRecord<Object,FlushNotification> rec = newFlushNotification(OpsWorkerTest.CLIENT2_ID, flushOffsetData, flushOffsetOps, offset);
        opsConsumer.addRecord(Utils.cast(rec));
        return rec;
    }

    @Test
    void testForwardCompatibility() {
        TopicPartition p = new TopicPartition(TOPIC_OPS, 0);
        opsWorker.flushQueues.get(p.partition()).setMaxOffset(0L);
        opsWorker.applyOpsTopicRecords(p, asList(new ConsumerRecord<>(TOPIC_OPS, p.partition(), 1L, null,
            new MapUpdate((byte)'Z', CLIENT1_ID, 100500, null, null, null))));
        opsWorker.applyOpsTopicRecords(p, asList(new ConsumerRecord<>(TOPIC_OPS, p.partition(), 2L, "key",
            new MapUpdate((byte)'Z', CLIENT1_ID, 100500, null, null, null))));
    }

    @Test
    void testTryFindLastFlushRecordEmpty() {
        opsConsumer.updateEndOffsets(singletonMap(opsPart, 0L));
        assertSame(OpsWorker.NOT_EXIST,
            opsWorker.tryFindLastFlushNotification(opsPart, 0L, Duration.ofMillis(1)));
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

        assertSame(OpsWorker.NOT_FOUND, opsWorker.tryFindLastFlushNotification(opsPart,
            opsConsumer.endOffsets(singleton(opsPart)).get(opsPart), Duration.ofMillis(1)));
    }

    @Test
    void testTryFindLastFlushRecord() {
        long offset = 1000;

        addFlushNotification(100500, offset - 1, offset++);

        for (int i = 0; i < FLUSH_MAX_OPS; i++)
            addPutRecord(CLIENT1_ID, offset++);

        ConsumerRecord<Object,FlushNotification> rec =
            addFlushNotification(200600, offset - 1, offset++);

        for (int i = 0; i < FLUSH_MAX_OPS - 1; i++)
            addPutRecord(CLIENT1_ID, offset++);

        opsConsumer.updateEndOffsets(singletonMap(opsPart, offset));

        assertSame(rec, opsWorker.tryFindLastFlushNotification(opsPart,
            opsConsumer.endOffsets(singleton(opsPart)).get(opsPart), Duration.ofMillis(1)));
    }

    @Test
    void testFindLastFlushRecordEmpty() {
        opsConsumer.updateEndOffsets(singletonMap(opsPart, 0L));
        assertSame(null,
            opsWorker.findLastFlushNotification(dataPart, opsPart, Utils.millis(1)));
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

        assertEquals(200600L, opsWorker.findLastFlushNotification(dataPart, opsPart, Utils.millis(1))
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
        opsWorker.flushQueues.get(opsPart.partition()).setMaxOffset(1000);

        opsWorker.applyOpsTopicRecords(opsPart, Arrays.asList(
            newPutRecord(CLIENT1_ID, 1001),
            newPutRecord(CLIENT2_ID, 1002),
            newPutRecord(CLIENT1_ID, 1003),
            Utils.cast(newFlushNotification(CLIENT1_ID, 100500, 1003, 1004)),
            newPutRecord(CLIENT1_ID, 1005),
            newPutRecord(CLIENT2_ID, 1006),
            newPutRecord(CLIENT1_ID, 1007),
            Utils.cast(newFlushNotification(CLIENT2_ID, 200700, 1007, 1008)),
            newPutRecord(CLIENT1_ID, 1009),
            newPutRecord(CLIENT1_ID, 1010)
        ));

        assertEquals(8, appliedUpdates.get());
        assertEquals(1, cleanQueue.size());
        assertEquals(200700, cleanQueue.poll().value().getFlushOffsetData());

        assertEquals(1, flushProducer.history().size());
        FlushRequest flushReq = flushProducer.history().get(0).value();
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
        opsWorker.flushQueues.get(opsPart.partition()).setMaxOffset(49);
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
            newPutRecord(CLIENT2_ID, 52),
            newPutRecord(CLIENT2_ID, 53),
            newPutRecord(CLIENT2_ID, 54),
            newPutRecord(CLIENT2_ID, 55),
            newPutRecord(CLIENT2_ID, 56),
            newPutRecord(CLIENT2_ID, 57),
            newPutRecord(CLIENT2_ID, 58),
            newPutRecord(CLIENT2_ID, 59),
            newPutRecord(CLIENT1_ID, 60),
            newPutRecord(CLIENT1_ID, 61)
        )))));

        assertEquals(12, appliedUpdates.get());

        List<ProducerRecord<Object,FlushRequest>> flushHistory = flushProducer.history();
        assertEquals(1, flushHistory.size());

        ProducerRecord<Object,FlushRequest> prepFlushReq = flushHistory.get(0);
        assertNull(prepFlushReq.key());

        FlushRequest flushReq = prepFlushReq.value();
        assertEquals(OP_FLUSH_REQUEST, flushReq.getOpType());
        assertEquals(CLIENT1_ID, flushReq.getClientId());
        assertEquals(60, flushReq.getFlushOffsetOps());
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