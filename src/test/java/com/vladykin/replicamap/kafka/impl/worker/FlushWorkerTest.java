package com.vladykin.replicamap.kafka.impl.worker;

import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.util.FlushQueue;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
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

import static com.vladykin.replicamap.base.ReplicaMapBase.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.base.ReplicaMapBase.OP_FLUSH_REQUEST;
import static com.vladykin.replicamap.kafka.impl.worker.OpsWorkerTest.CLIENT1_ID;
import static com.vladykin.replicamap.kafka.impl.worker.OpsWorkerTest.CLIENT2_ID;
import static com.vladykin.replicamap.kafka.impl.worker.OpsWorkerTest.TOPIC_DATA;
import static com.vladykin.replicamap.kafka.impl.worker.OpsWorkerTest.TOPIC_FLUSH;
import static com.vladykin.replicamap.kafka.impl.worker.OpsWorkerTest.TOPIC_OPS;
import static com.vladykin.replicamap.kafka.impl.worker.OpsWorkerTest.newFlushNotification;
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
    LazyList<Consumer<Object,OpMessage>> flushConsumers;

    MockConsumer<Object,Object> dataConsumer;
    MockProducer<Object,Object> dataProducer;
    MockProducer<Object,OpMessage> opsProducer;
    MockConsumer<Object,OpMessage> flushConsumer;

    List<FlushQueue> flushQueues;
    Queue<ConsumerRecord<Object,OpMessage>> cleanQueue;

    CompletableFuture<ReplicaMapManager> opsSteadyFut;
    FlushWorker flushWorker;

    TopicPartition flushPart = new TopicPartition(TOPIC_FLUSH, 0);
    TopicPartition dataPart = new TopicPartition(TOPIC_DATA, 0);

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
            MAX_POLL_TIMEOUT,
            dataProducers,
            this::createDataProducer,
            flushConsumers,
            this::createFlushConsumer
        );
    }

    Producer<Object,Object> createDataProducer(int part) {
        return dataProducer;
    }

    Consumer<Object,OpMessage> createFlushConsumer() {
        return flushConsumer;
    }

    @Test
    void testProcessCleanRequests() {
        FlushQueue flushQueue = flushQueues.get(0);

        flushQueue.add(null, null, 100, true, false);
        flushQueue.add(null, null, 101, true, false);
        flushQueue.add(null, null, 102, true, false);
        flushQueue.add(null, null, 103, true, false);
        flushQueue.add(null, null, 104, true, false);
        flushQueue.add(null, null, 105, true, false);
        flushQueue.add(null, null, 106, true, false);

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

        initFlushConsumer(101, 97);
        flushWorker.initDataProducers(singleton(flushPart));
        assertTrue(flushWorker.unprocessedFlushRequests.isEmpty());
        flushWorker.initUnprocessedFlushRequests(flushPart, -1, -1);
        assertFalse(flushWorker.unprocessedFlushRequests.isEmpty());
        assertFalse(flushWorker.processFlushRequests(0)); // No data in flush queue.

        FlushQueue flushQueue = flushQueues.get(0);

        flushQueue.add("a", "a", 98, true, true);
        flushQueue.add("b", "b", 99, true, true);
        flushQueue.add("a", "x", 100, true, true);
        flushQueue.add("b", "y", 101, true, true);
        flushQueue.add("a", "z", 102, true, true);

        assertEquals(5, flushQueue.size());

        flushWorker.flushConsumers.reset(0, flushConsumer);
        flushConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        initFlushConsumer(101, 98);
        flushConsumer.close();
        assertFalse(flushWorker.processFlushRequests(0)); // Exception on creating the consumer.
        assertTrue(flushWorker.unprocessedFlushRequests.isEmpty());

        flushConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        initFlushConsumer(101, 98);
        dataProducer = new MockProducer<>();
        flushWorker.resetDataProducers(singleton(flushPart));
        flushWorker.initDataProducers(singleton(flushPart));
        dataProducer.fenceProducer();
        flushWorker.initUnprocessedFlushRequests(flushPart, -1, -1);

        assertFalse(flushWorker.processFlushRequests(0));
        assertFalse(dataProducer.transactionCommitted());
        assertTrue(flushWorker.unprocessedFlushRequests.isEmpty()); // must be cleaned on fence

        flushWorker.flushConsumers.reset(0, flushWorker.flushConsumers.get(0, null));
        flushWorker.initUnprocessedFlushRequests(flushPart, -1, -1);
        dataProducer = new MockProducer<>();
        flushWorker.initDataProducers(singleton(flushPart));
        flushConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        initFlushConsumer(101, 98);

        assertTrue(flushWorker.processFlushRequests(0));
        assertTrue(dataProducer.transactionCommitted());
        assertEquals(1, flushQueue.size());

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

        ProducerRecord<Object,OpMessage> flushNotifRec = ops.get(0);
        assertNull(flushNotifRec.key());

        OpMessage flushNotif = flushNotifRec.value();
        assertEquals(OP_FLUSH_NOTIFICATION, flushNotif.getOpType());
        assertEquals(CLIENT1_ID, flushNotif.getClientId());
        assertEquals(101, flushNotif.getFlushOffsetOps());
        assertEquals(1, flushNotif.getFlushOffsetData());

        dataProducer.fenceProducer();
        initFlushConsumer(102, 101);
        assertTrue(flushWorker.unprocessedFlushRequests.get(flushPart).isEmpty());

        assertEquals(1, flushQueue.size());
        assertFalse(flushWorker.processFlushRequests(0));
        assertEquals(1, flushQueue.size());

        dataProducer = new MockProducer<>();
        flushWorker.initDataProducers(singleton(flushPart));
        flushConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        initFlushConsumer(102, 101);
        flushConsumer.setPollException(new KafkaException("test"));

        assertFalse(flushWorker.processFlushRequests(0));
        assertEquals(1, flushQueue.size());

        opsProducer.clear();
        dataProducer = new MockProducer<>();
        flushWorker.resetDataProducers(singleton(flushPart));
        flushWorker.initDataProducers(singleton(flushPart));
        flushConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        initFlushConsumer(102, 101);
        flushWorker.initUnprocessedFlushRequests(flushPart, -1, -1);
        assertTrue(flushWorker.processFlushRequests(0));
        assertEquals(0, flushQueue.size());
        assertEquals(1, dataProducer.history().size());
        assertEquals(1, opsProducer.history().size());
    }

    private void initFlushConsumer(long flushOffsetOps, long lastCleanOffsetOps) {
        flushConsumer.subscribe(singleton(TOPIC_FLUSH));
        flushConsumer.rebalance(singletonList(flushPart));
        flushConsumer.seek(flushPart, 770);

        flushConsumer.addRecord(new ConsumerRecord<>(TOPIC_FLUSH, 0, 777, null,
            new OpMessage(OP_FLUSH_REQUEST, CLIENT1_ID, 0L, flushOffsetOps - 1, -1L)));
        flushConsumer.addRecord(new ConsumerRecord<>(TOPIC_FLUSH, 0, 778, null,
            new OpMessage(OP_FLUSH_REQUEST, CLIENT1_ID, 0L, flushOffsetOps, lastCleanOffsetOps)));
    }

    private ConsumerRecord<Object,OpMessage> newFlushRequest(long offset, long flushOffsetOps) {
        return new ConsumerRecord<>(TOPIC_FLUSH, 0, offset, null,
            new OpMessage(OP_FLUSH_REQUEST, CLIENT1_ID, 0L, flushOffsetOps, 1000));
    }

    @Test
    void testLoadMaxCommittedFlushRequest() {
        flushConsumer.subscribe(singleton(TOPIC_FLUSH));
        flushConsumer.rebalance(singletonList(flushPart));

        flushConsumer.seek(flushPart, 1);
        assertNull(flushWorker.loadMaxCommittedFlushRequest(flushConsumer, flushPart, 0));

        flushConsumer.addRecord(newFlushRequest(100, 1011));
        flushConsumer.addRecord(newFlushRequest(101, 1017));
        flushConsumer.addRecord(newFlushRequest(102, 1015));
        flushConsumer.addRecord(newFlushRequest(103, 1014));
        flushConsumer.addRecord(newFlushRequest(104, 1013));
        flushConsumer.addRecord(newFlushRequest(105, 1010));
        flushConsumer.addRecord(newFlushRequest(106, 1009));
        flushConsumer.addRecord(newFlushRequest(107, 1008));
        flushConsumer.addRecord(newFlushRequest(108, 1007));
        flushConsumer.addRecord(newFlushRequest(109, 1006));
        flushConsumer.addRecord(newFlushRequest(110, 1005));
        flushConsumer.addRecord(newFlushRequest(111, 1004));
        flushConsumer.addRecord(newFlushRequest(112, 1018));
        flushConsumer.addRecord(newFlushRequest(113, 1019));

        flushConsumer.seek(flushPart, 113);
        OpMessage flush = flushWorker.loadMaxCommittedFlushRequest(flushConsumer, flushPart, 112).value();
        assertEquals(113, flushConsumer.position(flushPart));

        assertEquals(1004, flush.getFlushOffsetOps());
    }
}