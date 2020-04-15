package com.vladykin.replicamap.kafka.impl.worker.flush;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.kafka.impl.msg.FlushNotification;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import com.vladykin.replicamap.kafka.impl.worker.Worker;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.kafka.impl.util.Utils.MIN_DURATION_MS;
import static com.vladykin.replicamap.kafka.impl.util.Utils.millis;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

/**
 * Processes flush requests (flushes the collected updates to the `data` topic)
 * and clean requests (cleans the flush queue).
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class FlushWorker extends Worker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FlushWorker.class);

    public static final String OPS_OFFSET_HEADER = "replicamap.ops";

    protected final long clientId;

    protected final String dataTopic;
    protected final String opsTopic;
    protected final String flushTopic;

    protected final String flushConsumerGroupId;
    protected final LazyList<Consumer<Object,FlushRequest>> flushConsumers;
    protected final Supplier<Consumer<Object,FlushRequest>> flushConsumerFactory;

    protected final LazyList<Producer<Object,Object>> dataProducers;
    protected final IntFunction<Producer<Object,Object>> dataProducerFactory;

    protected final Producer<Object,OpMessage> opsProducer;

    protected final Queue<ConsumerRecord<Object,FlushNotification>> cleanQueue;
    protected final List<FlushQueue> flushQueues;

    protected final CompletableFuture<ReplicaMapManager> opsSteadyFut;

    protected final long maxPollTimeout;

    protected final Map<TopicPartition,UnprocessedFlushRequests> unprocessedFlushRequests = new HashMap<>();
    protected final short[] allowedPartitions;
    protected final Set<TopicPartition> assignedPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    protected final LongAdder receivedFlushRequests;
    protected final LongAdder successfulFlushes;

    public FlushWorker(
        long clientId,
        String dataTopic,
        String opsTopic,
        String flushTopic,
        int workerId,
        String flushConsumerGroupId,
        Producer<Object,OpMessage> opsProducer,
        List<FlushQueue> flushQueues,
        Queue<ConsumerRecord<Object,FlushNotification>> cleanQueue,
        CompletableFuture<ReplicaMapManager> opsSteadyFut,
        LongAdder receivedFlushRequests,
        LongAdder successfulFlushes,
        long maxPollTimeout,
        short[] allowedPartitions,
        LazyList<Producer<Object,Object>> dataProducers,
        IntFunction<Producer<Object,Object>> dataProducerFactory,
        LazyList<Consumer<Object,FlushRequest>> flushConsumers,
        Supplier<Consumer<Object,FlushRequest>> flushConsumerFactory
    ) {
        super("replicamap-flush-" + dataTopic + "-" +
            Long.toHexString(clientId), workerId);

        this.clientId = clientId;
        this.dataTopic = dataTopic;
        this.opsTopic = opsTopic;
        this.flushTopic = flushTopic;
        this.flushConsumerGroupId = flushConsumerGroupId;
        this.opsProducer = opsProducer;
        this.flushQueues = flushQueues;
        this.cleanQueue = cleanQueue;
        this.opsSteadyFut = opsSteadyFut;
        this.receivedFlushRequests = receivedFlushRequests;
        this.successfulFlushes = successfulFlushes;
        this.maxPollTimeout = maxPollTimeout;
        this.allowedPartitions = allowedPartitions;
        this.dataProducers = dataProducers;
        this.dataProducerFactory = dataProducerFactory;
        this.flushConsumers = flushConsumers;
        this.flushConsumerFactory = flushConsumerFactory;
    }

    @Override
    public void close() {
        Utils.close(dataProducers);
        Utils.close(flushConsumers);
    }

    @Override
    protected void doRun() throws Exception {
        long pollTimeoutMs = 5;

        while (!isInterrupted()) {
            boolean flushed = processFlushRequests(pollTimeoutMs);
            boolean cleaned = processCleanRequests();

            pollTimeoutMs = updatePollTimeout(pollTimeoutMs, flushed, cleaned);
        }
    }

    @Override
    protected void interruptThread() {
        Utils.wakeup(() -> flushConsumers.get(0, null));
        super.interruptThread();
    }

    protected long updatePollTimeout(long pollTimeoutMs, boolean flushed, boolean cleaned) {
        return flushed || cleaned ? MIN_DURATION_MS : Math.min(pollTimeoutMs * 2, maxPollTimeout);
    }

    protected boolean processCleanRequests() {
        for (int cnt = 0;; cnt++) {
            ConsumerRecord<Object,FlushNotification> cleanRequest = cleanQueue.poll();

            if (cleanRequest == null || isInterrupted())
                return cnt > 0;

            FlushNotification op = cleanRequest.value();
            assert op.getOpType() == OP_FLUSH_NOTIFICATION && op.getClientId() != clientId: op;

            long flushOffsetOps = op.getFlushOffsetOps();
            int part = cleanRequest.partition();

            FlushQueue flushQueue = flushQueues.get(part);
            long cleanedCnt = flushQueue.clean(flushOffsetOps, "processCleanRequests");

            if (log.isDebugEnabled()) {
                log.debug("Processed clean request for partition {}, flushQueueSize: {}, cleanedCnt: {}, clean request: {}",
                    new TopicPartition(cleanRequest.topic(), cleanRequest.partition()), cleanedCnt,
                    flushQueue.size(), cleanRequest);
            }
        }
    }

    protected void clearUnprocessedFlushRequestsUntil(TopicPartition flushPart, long maxOffsetOps) {
        UnprocessedFlushRequests flushReqs = unprocessedFlushRequests.get(flushPart);
        if (flushReqs != null && !flushReqs.isEmpty())
            flushReqs.clearUntil(maxOffsetOps);
        // Do not remove empty flushReqs from unprocessedFlushRequests,
        // otherwise we will reload flush history each time.
    }

    protected boolean awaitOpsWorkersSteady(long pollTimeoutMs) throws ExecutionException, InterruptedException {
        try {
            opsSteadyFut.get(pollTimeoutMs, TimeUnit.MILLISECONDS);
            return true;
        }
        catch (TimeoutException e) {
            return false;
        }
    }

    protected boolean processFlushRequests(long pollTimeoutMs) throws InterruptedException, ExecutionException {
        // We start consuming flush requests only when the ops workers are steady,
        // because otherwise we may have not enough data to flush.
        if (!awaitOpsWorkersSteady(pollTimeoutMs))
            return false;

        Consumer<Object,FlushRequest> flushConsumer;
        try {
            flushConsumer = flushConsumers.get(0, this::newFlushConsumer);
        }
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Failed to create flush consumer for topic: " + flushTopic, e);

            return false;
        }

        ConsumerRecords<Object,FlushRequest> recs;
        try {
            try {
                recs = flushConsumer.poll(millis(pollTimeoutMs));
            }
            catch (NoOffsetForPartitionException e) {
                initFlushConsumerOffset(flushConsumer, e); // Needed when it is a new empty flush topic.
                recs = flushConsumer.poll(millis(pollTimeoutMs));
            }

            // Add new records to unprocessed set and load history if it is the first flush for the partition.
            for (TopicPartition flushPart : recs.partitions()) {
                // Ignore disallowed partitions, they usually should not come except the mixed ReplicaMap versions case.
                if (isAllowedPartition(flushPart))
                    collectUnprocessedFlushRequests(flushConsumer, flushPart, recs.records(flushPart));
            }
        }
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Failed to poll flush consumer for topic: " + flushTopic, e);

            resetAll(flushConsumer);
            return false;
        }

        boolean flushed = false;

        // Try process the unprocessed set.
        for (Map.Entry<TopicPartition,UnprocessedFlushRequests> entry : unprocessedFlushRequests.entrySet())
             flushed |= flushPartition(flushConsumer, entry.getKey(), entry.getValue());

        return flushed;
    }

    protected boolean isAllowedPartition(TopicPartition part) {
        return allowedPartitions == null || Utils.contains(allowedPartitions, (short)part.partition());
    }

    protected void initFlushConsumerOffset(
        Consumer<Object,FlushRequest> flushConsumer,
        NoOffsetForPartitionException e
    ) {
        Set<TopicPartition> partitions = e.partitions();
        flushConsumer.seekToBeginning(partitions);

        // Check that it is just a fresh flush topic,
        // otherwise we must always have committed offset for the flush consumer group.
        for (TopicPartition partition : partitions) {
            if (flushConsumer.position(partition) != 0L)
                throw new ReplicaMapException("Failed to init flush consumer offset.", e);
        }
    }

    protected void collectUnprocessedFlushRequests(
        Consumer<Object,FlushRequest> flushConsumer,
        TopicPartition flushPart,
        List<ConsumerRecord<Object,FlushRequest>> flushReqsList
    ) {
        if (flushReqsList.isEmpty())
            return;

        UnprocessedFlushRequests flushReqs = unprocessedFlushRequests.get(flushPart);

        // If it is the first batch of records for this partition we need to initialize the processing for it.
        if (flushReqs == null) {
            long firstRecOffset = flushReqsList.get(0).offset();
            boolean loadLastCommittedFlushReq = firstRecOffset > 0;

            flushReqs = new UnprocessedFlushRequests(flushPart, firstRecOffset, !loadLastCommittedFlushReq);
            unprocessedFlushRequests.put(flushPart, flushReqs);

            if (loadLastCommittedFlushReq) {
                // We seek for the previous record which is the last committed flush request to make sure that
                // there will be no issues if the subsequent flush requests will arrive out of order.
                flushConsumer.seek(flushPart, firstRecOffset - 1);
                return;
            }
        }

        int cnt = flushReqsList.size();
        if (!flushReqs.isInitialized()) cnt--; // Ignore the last committed flush request.
        receivedFlushRequests.add(cnt);

        flushReqs.addFlushRequests(flushReqsList);
    }

    protected void resetAll(Consumer<Object,FlushRequest> flushConsumer) {
        assignedPartitions.clear();
        unprocessedFlushRequests.clear();

        for (int part = 0; part < dataProducers.size(); part++)
            resetDataProducer(new TopicPartition(flushTopic, part));

        flushConsumers.reset(0, flushConsumer);
    }

    protected boolean flushPartition(
        Consumer<Object,FlushRequest> flushConsumer,
        TopicPartition flushPart,
        UnprocessedFlushRequests flushReqs
    ) {
        if (flushReqs.isEmpty())
            return false;

        // Here we must not modify any local state until transaction is successfully committed.
        int part = flushPart.partition();
        FlushQueue flushQueue = flushQueues.get(part);
        TopicPartition dataPart = flushQueue.getDataPartition();

        log.debug("Processing flush requests for partition {}: {}", dataPart, flushReqs);

        flushQueue.clean(flushReqs.getMaxCleanOffsetOps(), "flushPartitionBegin");
        FlushQueue.Batch dataBatch = flushQueue.collect(flushReqs.getFlushOffsetOpsStream());

        if (dataBatch == null || dataBatch.isEmpty()) {
            // Check if we are too far behind.
            if (flushReqs.size() > 1) // TODO add to config
                resetAll(flushConsumer);

            return false; // Not enough data.
        }

        long flushOffsetOps = dataBatch.getMaxOffset();
        OffsetAndMetadata flushConsumerOffset = flushReqs.getFlushConsumerOffsetToCommit(flushOffsetOps);

        int dataBatchSize = dataBatch.size();
        int collectedAll = dataBatch.getCollectedAll();

        if (log.isDebugEnabled()) {
            log.debug("Collected batch for partition {}, dataBatchSize: {}, collectedAll: {}" +
                    ", flushConsumerOffset: {}, flushOffsetOps: {}",
                dataPart, dataBatchSize, collectedAll, flushConsumerOffset, flushOffsetOps);
        }

        Producer<Object,Object> dataProducer;
        long flushOffsetData;

        try {
            dataProducer = dataProducers.get(part, null);
            flushOffsetData = flushTx(dataProducer, dataBatch, flushPart, flushConsumerOffset, flushOffsetOps);
        }
        catch (Exception e) {
            boolean fenced = e instanceof ProducerFencedException;

            if (fenced || Utils.isInterrupted(e)) {
                log.warn("Failed to flush data for partition {}, flushConsumerOffset: {}" +
                        ", flushOffsetOps: {}, flushQueueSize: {}, reason: {}",
                    dataPart, flushConsumerOffset, flushOffsetOps, flushQueue.size(), Utils.getMessage(e));
            }
            else {
                log.error("Failed to flush data for partition " + dataPart + ", flushConsumerOffset: " + flushConsumerOffset +
                    ", flushOffsetOps: " + flushOffsetOps + ", exception:", e);
            }

            if (fenced)
                resetDataProducer(flushPart);
            else
                resetAll(flushConsumer);

            return false; // No other handling, the next flush will be our retry.
        }

        if (log.isDebugEnabled()) {
            log.debug("Committed tx for partition {}, flushOffsetData: {}, flushOffsetOps: {}, dataProducer: {}, dataBatch: {}",
                dataPart, flushOffsetData, flushOffsetOps, dataProducer, dataBatch);
        }

        clearUnprocessedFlushRequestsUntil(flushPart, flushOffsetOps);

        if (flushQueue.clean(flushOffsetOps, "flushPartitionEnd") > 0) {
            sendFlushNotification(dataPart, flushOffsetData, flushOffsetOps);

            if (log.isDebugEnabled()) {
                log.debug("Successfully flushed {} data records for partition {}, flushOffsetOps: {}" +
                        ", flushOffsetData: {}, flushQueueSize: {}",
                    dataBatchSize, dataPart, flushOffsetOps, flushOffsetData, flushQueue.size());
            }
        }

        successfulFlushes.increment();
        return true;
    }

    protected long flushTx(
        Producer<Object,Object> dataProducer,
        FlushQueue.Batch dataBatch,
        TopicPartition flushPart,
        OffsetAndMetadata flushConsumerOffset,
        long flushOffsetOps
    ) throws ExecutionException, InterruptedException {
        assert !dataBatch.isEmpty();

        int part = flushPart.partition();
        Future<RecordMetadata> lastDataRecMetaFut = null;

        int i = 0;

        dataProducer.beginTransaction();
        for (Map.Entry<Object,Object> entry : dataBatch.entrySet()) {
            lastDataRecMetaFut = dataProducer.send(new ProducerRecord<>(
                dataTopic, part, entry.getKey(), entry.getValue(),
                ++i != dataBatch.size() ? null : newOpsOffsetHeader(flushOffsetOps)));
        }
        dataProducer.sendOffsetsToTransaction(singletonMap(flushPart, flushConsumerOffset), flushConsumerGroupId);
        dataProducer.commitTransaction();

        // We check that the batch is not empty, thus we have to have the last record non-null here.
        // Since ReplicaMap checks preconditions locally before sending anything to Kafka,
        // it is impossible to have a large number of failed update attempts,
        // there must always be a winner, so we will be able to commit periodically.
        assert lastDataRecMetaFut != null;
        return lastDataRecMetaFut.get().offset();
    }

    public static Iterable<Header> newOpsOffsetHeader(long offset) {
        return singleton(new RecordHeader(OPS_OFFSET_HEADER, Utils.serializeVarlong(offset)));
    }

    protected void sendFlushNotification(TopicPartition dataPart, long flushOffsetData, long flushOffsetOps) {
        ProducerRecord<Object,OpMessage> flushNotification = new ProducerRecord<>(opsTopic, dataPart.partition(), null,
            newFlushNotification(flushOffsetData, flushOffsetOps));

        if (log.isDebugEnabled())
            log.debug("Sending flush notification for partition {}: {}", dataPart, flushNotification);

        try {
            opsProducer.send(flushNotification);
        }
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Failed to send flush notification for partition " + dataPart + ": " + flushNotification, e);
        }
    }

    protected FlushNotification newFlushNotification(long flushOffsetData, long flushOffsetOps) {
        return new FlushNotification(clientId, flushOffsetData, flushOffsetOps);
    }

    protected Producer<Object,Object> newDataProducer(int part) {
        Producer<Object,Object> p = dataProducerFactory.apply(part);
        p.initTransactions();
        return p;
    }

    protected Consumer<Object,FlushRequest> newFlushConsumer(int ignore) {
        Consumer<Object,FlushRequest> c = flushConsumerFactory.get();
        c.subscribe(singleton(flushTopic), new PartitionRebalanceListener());
        return c;
    }

    protected void clearUnprocessedFlushRequests(Collection<TopicPartition> flushPartitions) {
        log.debug("Clearing unprocessed flush requests for partitions: {}", flushPartitions);

        for (TopicPartition part : flushPartitions)
            unprocessedFlushRequests.remove(part);
    }

    protected void initDataProducers(Collection<TopicPartition> flushPartitions) {
        if (log.isDebugEnabled())
            log.debug("Initializing data producers for partitions: {}", convert(flushPartitions, dataTopic));

        for (TopicPartition partition : flushPartitions) {
            int part = partition.partition();

            if (dataProducers.get(part, null) != null)
                throw new IllegalStateException("Producer exists for partition: " + new TopicPartition(dataTopic, part));

            dataProducers.get(part, this::newDataProducer);
        }
    }

    public Set<TopicPartition> getAssignedPartitions() {
        return assignedPartitions;
    }

    protected List<TopicPartition> convert(Collection<TopicPartition> partitions, String topic) {
        return partitions.stream()
            .map(p -> new TopicPartition(topic, p.partition()))
            .collect(toList());
    }

    protected void resetDataProducers(Collection<TopicPartition> flushPartitions) {
        if (log.isDebugEnabled())
            log.debug("Resetting data producers for partitions: {}", convert(flushPartitions, dataTopic));

        for (TopicPartition flushPart : flushPartitions)
            resetDataProducer(flushPart);
    }

    protected void resetDataProducer(TopicPartition flushPart) {
        int part = flushPart.partition();
        Producer<Object,Object> dataProducer = dataProducers.get(part, null);

        if (dataProducer != null)
            dataProducers.reset(part, dataProducer);

        unprocessedFlushRequests.remove(flushPart);
    }

    /**
     * We initialize transactional data producer in the listener to make sure
     * it happens before the fetch, otherwise races are possible on rebalancing.
     */
    protected class PartitionRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty())
                return;

            log.debug("Flush partitions assigned: {}", partitions);
            clearUnprocessedFlushRequests(partitions);
            initDataProducers(partitions);
            assignedPartitions.addAll(partitions);
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty())
                return;

            log.debug("Flush partitions revoked: {}", partitions);
            clearUnprocessedFlushRequests(partitions);
            resetDataProducers(partitions);
            assignedPartitions.removeAll(partitions);
        }
    }
}
