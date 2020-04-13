package com.vladykin.replicamap.kafka.impl.worker.flush;

import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.kafka.impl.msg.FlushNotification;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.LongAdder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.kafka.impl.worker.ops.OpsWorkerTest.CLIENT1_ID;
import static com.vladykin.replicamap.kafka.impl.worker.ops.OpsWorkerTest.CLIENT2_ID;
import static com.vladykin.replicamap.kafka.impl.worker.ops.OpsWorkerTest.TOPIC_DATA;
import static com.vladykin.replicamap.kafka.impl.worker.ops.OpsWorkerTest.TOPIC_FLUSH;
import static com.vladykin.replicamap.kafka.impl.worker.ops.OpsWorkerTest.TOPIC_OPS;
import static com.vladykin.replicamap.kafka.impl.worker.ops.OpsWorkerTest.newFlushNotification;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class FlushWorkerTest {
    static final long MAX_POLL_TIMEOUT = 20;

    LazyList<Consumer<Object,Object>> dataConsumers;
    LazyList<Producer<Object,Object>> dataProducers;
    LazyList<Consumer<Object,FlushRequest>> flushConsumers;

    MockConsumer<Object,Object> dataConsumer;
    MockProducer<Object,Object> dataProducer;
    MockProducer<Object,OpMessage> opsProducer;
    MockConsumer<Object,FlushRequest> flushConsumer;

    List<FlushQueue> flushQueues;
    Queue<ConsumerRecord<Object,FlushNotification>> cleanQueue;

    CompletableFuture<ReplicaMapManager> opsSteadyFut;
    FlushWorker flushWorker;

    TopicPartition flushPart = new TopicPartition(TOPIC_FLUSH, 0);
    TopicPartition dataPart = new TopicPartition(TOPIC_DATA, 0);

    LongAdder receivedFlushRequests = new LongAdder();
    LongAdder successfulFlushes = new LongAdder();

    @BeforeEach
    void beforeEachTest() {
        dataConsumers = new LazyList<>(1);
        dataProducers = new LazyList<>(1);
        flushConsumers = new LazyList<>(1);

        flushConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        dataConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        dataProducer = new MockProducer<>();
        opsProducer = new MockProducer<>();

        flushQueues = singletonList(new FlushQueue(dataPart));
        cleanQueue = new ConcurrentLinkedQueue<>();

        opsSteadyFut = new CompletableFuture<>();

        flushWorker = new FlushWorker(
            CLIENT1_ID, TOPIC_DATA, TOPIC_OPS, TOPIC_FLUSH,
            0,
            "flush-consumer-group-id",
            opsProducer,
            flushQueues,
            cleanQueue,
            opsSteadyFut,
            receivedFlushRequests,
            successfulFlushes,
            MAX_POLL_TIMEOUT,
            null,
            dataProducers,
            this::createDataProducer,
            flushConsumers,
            this::createFlushConsumer
        );
    }

    Producer<Object,Object> createDataProducer(int part) {
        return dataProducer;
    }

    Consumer<Object,FlushRequest> createFlushConsumer() {
        return flushConsumer;
    }

    @Test
    void testProcessCleanRequests() {
        FlushQueue flushQueue = flushQueues.get(0);
        flushQueue.setMaxOffset(99);

        flushQueue.add(1, null, 100, false);
        flushQueue.add(2, null, 101, false);
        flushQueue.add(3, null, 102, false);
        flushQueue.add(4, null, 103, false);
        flushQueue.add(5, null, 104, false);
        flushQueue.add(6, null, 105, false);
        flushQueue.add(7, null, 106, false);

        assertEquals(7, flushQueue.size());

        cleanQueue.add(newFlushNotification(CLIENT2_ID, 100500, 101, 107));

        flushWorker.processCleanRequests();

        assertEquals(5, flushQueue.size());

        cleanQueue.add(newFlushNotification(CLIENT2_ID, 100600, 103, 108));
        cleanQueue.add(newFlushNotification(CLIENT2_ID, 100700, 105, 109));

        flushWorker.processCleanRequests();

        assertEquals(1, flushQueue.size());
    }

    @Test
    void testUpdatePollTimeout() {
        assertEquals(1, flushWorker.updatePollTimeout(15, false, true));
        assertEquals(1, flushWorker.updatePollTimeout(15, true, false));
        assertEquals(1, flushWorker.updatePollTimeout(15, true, true));

        long timeout = 1;
        assertEquals(2, timeout = flushWorker.updatePollTimeout(timeout, false, false));
        assertEquals(4, timeout = flushWorker.updatePollTimeout(timeout, false, false));
        assertEquals(8, timeout = flushWorker.updatePollTimeout(timeout, false, false));
        assertEquals(16, timeout = flushWorker.updatePollTimeout(timeout, false, false));
        assertEquals(20, timeout = flushWorker.updatePollTimeout(timeout, false, false));
        assertEquals(20, timeout = flushWorker.updatePollTimeout(timeout, false, false));

        assertEquals(1, timeout = flushWorker.updatePollTimeout(timeout, false, true));
        assertEquals(2, timeout = flushWorker.updatePollTimeout(timeout, false, false));
        assertEquals(4, timeout = flushWorker.updatePollTimeout(timeout, false, false));
        assertEquals(8, flushWorker.updatePollTimeout(timeout, false, false));
    }

    @Test
    void testAwaitOpsSteady() throws ExecutionException, InterruptedException {
        assertFalse(flushWorker.awaitOpsWorkersSteady(0));
        assertFalse(flushWorker.awaitOpsWorkersSteady(1));
        opsSteadyFut.complete(null);
        assertTrue(flushWorker.awaitOpsWorkersSteady(1));
    }

    @Test
    void testProcessFlushRequests() throws ExecutionException, InterruptedException {
        assertFalse(flushWorker.processFlushRequests(0)); // Not steady.
        opsSteadyFut.complete(null);

        assertFalse(flushWorker.processFlushRequests(0)); // No flush requests.

        initFlushConsumer(777,101, 97);
        flushWorker.initDataProducers(singleton(flushPart));
        assertTrue(flushWorker.unprocessedFlushRequests.isEmpty());
        assertFalse(flushWorker.processFlushRequests(0)); // No data in flush queue.
        assertTrue(flushWorker.unprocessedFlushRequests.get(flushPart).isEmpty());
        assertFalse(flushWorker.unprocessedFlushRequests.get(flushPart).isInitialized());

        FlushQueue flushQueue = flushQueues.get(0);
        flushQueue.setMaxOffset(97);

        flushQueue.add("a", "a", 98, true);
        flushQueue.add("b", "b", 99, true);
        flushQueue.add("a", "x", 100, true);
        flushQueue.add("b", "y", 101, true);
        flushQueue.add("a", "z", 102, true);

        assertEquals(5, flushQueue.size());

        flushWorker.flushConsumers.reset(0, flushConsumer);
        flushConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        initFlushConsumer(777,101, 98);
        flushConsumer.close();
        assertFalse(flushWorker.processFlushRequests(0)); // Exception on creating the consumer.
        assertTrue(flushWorker.unprocessedFlushRequests.get(flushPart).isEmpty());
        assertFalse(flushWorker.unprocessedFlushRequests.get(flushPart).isInitialized());

        flushConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        initFlushConsumer(777, 101, 98);
        dataProducer = new MockProducer<>();
        flushWorker.resetDataProducers(singleton(flushPart));
        flushWorker.initDataProducers(singleton(flushPart));
        dataProducer.fenceProducer();

        assertFalse(flushWorker.processFlushRequests(0));
        assertFalse(dataProducer.transactionCommitted());
        assertTrue(flushWorker.unprocessedFlushRequests.get(flushPart).isEmpty());
        assertFalse(flushWorker.unprocessedFlushRequests.get(flushPart).isInitialized());

        flushWorker.resetAll(flushWorker.flushConsumers.get(0, null));
        assertTrue(flushWorker.unprocessedFlushRequests.isEmpty());
        dataProducer = new MockProducer<>();
        flushWorker.initDataProducers(singleton(flushPart));
        flushConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        initFlushConsumer(778, 101, 98);

        assertEquals(0, successfulFlushes.sum());
        assertFalse(flushWorker.processFlushRequests(0)); // Load last committed flush request.
        assertEquals(0, successfulFlushes.sum());
        initFlushConsumer(777, 101, 98);
        assertTrue(flushWorker.processFlushRequests(0));

        assertTrue(dataProducer.transactionCommitted());
        assertEquals(1, flushQueue.size());
        assertEquals(1, successfulFlushes.sum());

        List<ProducerRecord<Object,Object>> data = dataProducer.history();
        assertEquals(2, data.size());
        for (ProducerRecord<Object,Object> rec : data) {
            if ("a".equals(rec.key()))
                assertEquals("x", rec.value());
            else if ("b".equals(rec.key()))
                assertEquals("y", rec.value());
            else
                fail("Unknown key: " + rec.key());
        }

        List<ProducerRecord<Object,OpMessage>> ops = opsProducer.history();
        assertEquals(1, ops.size());

        ProducerRecord<Object,FlushNotification> flushNotifRec = Utils.cast(ops.get(0));
        assertNull(flushNotifRec.key());

        FlushNotification flushNotif = flushNotifRec.value();
        assertEquals(OP_FLUSH_NOTIFICATION, flushNotif.getOpType());
        assertEquals(CLIENT1_ID, flushNotif.getClientId());
        assertEquals(101, flushNotif.getFlushOffsetOps());
        assertEquals(1, flushNotif.getFlushOffsetData());

        dataProducer.fenceProducer();
        initFlushConsumer(777, 102, 101);
        assertTrue(flushWorker.unprocessedFlushRequests.get(flushPart).isEmpty());

        assertEquals(1, flushQueue.size());
        assertFalse(flushWorker.processFlushRequests(0));
        assertEquals(1, flushQueue.size());
        assertEquals(1, successfulFlushes.sum());

        dataProducer = new MockProducer<>();
        flushWorker.initDataProducers(singleton(flushPart));
        flushConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        initFlushConsumer(777, 102, 101);
        flushConsumer.setPollException(new KafkaException("test"));

        assertFalse(flushWorker.processFlushRequests(0));
        assertEquals(1, flushQueue.size());
        assertEquals(1, successfulFlushes.sum());

        opsProducer.clear();
        dataProducer = new MockProducer<>();
        flushWorker.resetDataProducers(singleton(flushPart));
        flushWorker.initDataProducers(singleton(flushPart));
        flushConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        initFlushConsumer(0, 102, 101);
        assertTrue(flushWorker.processFlushRequests(0));
        assertEquals(0, flushQueue.size());
        assertEquals(1, dataProducer.history().size());
        assertEquals(1, opsProducer.history().size());
        assertEquals(2, successfulFlushes.sum());
    }

    private void initFlushConsumer(long offset, long flushOffsetOps, long lastCleanOffsetOps) {
        flushConsumer.subscribe(singleton(TOPIC_FLUSH));
        flushConsumer.rebalance(singletonList(flushPart));
        flushConsumer.seek(flushPart, offset - 1);

        flushConsumer.addRecord(new ConsumerRecord<>(TOPIC_FLUSH, 0, offset, null,
            new FlushRequest(CLIENT1_ID, flushOffsetOps - 1, -1L)));
        flushConsumer.addRecord(new ConsumerRecord<>(TOPIC_FLUSH, 0, offset + 1, null,
            new FlushRequest(CLIENT1_ID, flushOffsetOps, lastCleanOffsetOps)));
    }
}