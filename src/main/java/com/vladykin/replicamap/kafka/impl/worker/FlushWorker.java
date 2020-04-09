package com.vladykin.replicamap.kafka.impl.worker;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.kafka.impl.FlushQueue;
import com.vladykin.replicamap.kafka.impl.UnprocessedFlushRequests;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vladykin.replicamap.base.ReplicaMapBase.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.kafka.impl.util.Utils.millis;
import static com.vladykin.replicamap.kafka.impl.util.Utils.seconds;
import static com.vladykin.replicamap.kafka.impl.util.Utils.trace;
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

    protected final long clientId;

    protected final String dataTopic;
    protected final String opsTopic;
    protected final String flushTopic;

    protected final String flushConsumerGroupId;
    protected final LazyList<Consumer<Object,OpMessage>> flushConsumers;
    protected final Supplier<Consumer<Object,OpMessage>> flushConsumerFactory;

    protected final LazyList<Producer<Object,Object>> dataProducers;
    protected final IntFunction<Producer<Object,Object>> dataProducerFactory;

    protected final Producer<Object,OpMessage> opsProducer;

    protected final Queue<ConsumerRecord<Object,OpMessage>> cleanQueue;
    protected final List<FlushQueue> flushQueues;

    protected final CompletableFuture<ReplicaMapManager> opsSteadyFut;

    protected final long maxPollTimeout;

    protected final Map<TopicPartition,UnprocessedFlushRequests> unprocessedFlushRequests = new HashMap<>();
    protected final short[] allowedPartitions;

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
        Queue<ConsumerRecord<Object,OpMessage>> cleanQueue,
        CompletableFuture<ReplicaMapManager> opsSteadyFut,
        LongAdder receivedFlushRequests,
        LongAdder successfulFlushes,
        long maxPollTimeout,
        short[] allowedPartitions,
        LazyList<Producer<Object,Object>> dataProducers,
        IntFunction<Producer<Object,Object>> dataProducerFactory,
        LazyList<Consumer<Object,OpMessage>> flushConsumers,
        Supplier<Consumer<Object,OpMessage>> flushConsumerFactory
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
        long pollTimeoutMs = 1;

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
        return flushed || cleaned ? 1 : Math.min(pollTimeoutMs * 2, maxPollTimeout);
    }

    protected boolean processCleanRequests() {
        for (int cnt = 0;; cnt++) {
            ConsumerRecord<Object,OpMessage> cleanRequest = cleanQueue.poll();

            if (cleanRequest == null || isInterrupted())
                return cnt > 0;

            OpMessage op = cleanRequest.value();
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

        Consumer<Object,OpMessage> flushConsumer;
        try {
            flushConsumer = flushConsumers.get(0, this::newFlushConsumer);
        }
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Failed to create flush consumer for topic " + flushTopic, e);

            return false;
        }

        ConsumerRecords<Object,OpMessage> recs;
        try {
            try {
                recs = flushConsumer.poll(millis(pollTimeoutMs));
            }
            catch (NoOffsetForPartitionException e) {
                initFlushConsumerOffset(flushConsumer, e);
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
        Consumer<Object,OpMessage> flushConsumer,
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
        Consumer<Object,OpMessage> flushConsumer,
        TopicPartition flushPart,
        List<ConsumerRecord<Object,OpMessage>> partRecs
    ) {
        UnprocessedFlushRequests flushReqs = unprocessedFlushRequests.get(flushPart);

        // If it is the first batch of records for this partition we need to initialize the processing for it.
        if (flushReqs == null) {
            // Load flush history and clean the flushQueue until the max committed historical offset,
            // after that FlushQueue will not accept outdated records.
            long firstRecOffset = partRecs.get(0).offset();
            ConsumerRecord<Object,OpMessage> maxCommittedFlushReq =
                loadMaxCommittedFlushRequest(flushConsumer, flushPart, firstRecOffset);

            long maxFlushOffsetOps = -1L;
            if (maxCommittedFlushReq != null) {
                trace.trace("{} firstRecOffset={}, maxCommittedFlushReq={}",
                    flushPart, firstRecOffset, maxCommittedFlushReq);

                maxFlushOffsetOps = maxCommittedFlushReq.value().getFlushOffsetOps();
                flushQueues.get(flushPart.partition()).clean(maxFlushOffsetOps, "maxHistory");
            }

            long maxFlushReqOffset = partRecs.get(0).offset() - 1L; // Right before the first received record.
            flushReqs = initUnprocessedFlushRequests(flushPart, maxFlushReqOffset, maxFlushOffsetOps);
        }

        receivedFlushRequests.add(partRecs.size());
        flushReqs.addFlushRequests(flushPart, partRecs);
    }

    protected void resetAll(Consumer<Object,OpMessage> flushConsumer) {
        unprocessedFlushRequests.clear();

        for (int part = 0; part < dataProducers.size(); part++)
            resetDataProducer(new TopicPartition(flushTopic, part));

        flushConsumers.reset(0, flushConsumer);
    }

    protected UnprocessedFlushRequests initUnprocessedFlushRequests(
        TopicPartition flushPart,
        long maxFlushReqOffset,
        long maxFlushOffsetOps
    ) {
        if (log.isDebugEnabled()) {
            log.debug("Init unprocessed flush requests for partition {}, maxFlushReqOffset: {}, maxFlushOffsetOps: {}",
                flushPart, maxFlushReqOffset, maxFlushOffsetOps);
        }

        UnprocessedFlushRequests flushRequests = new UnprocessedFlushRequests(maxFlushReqOffset, maxFlushOffsetOps);

        if (unprocessedFlushRequests.put(flushPart, flushRequests) != null)
            throw new IllegalStateException("Unprocessed flush requests already initialized for partition: " + flushPart);

        return flushRequests;
    }

    protected ConsumerRecord<Object,OpMessage> loadMaxCommittedFlushRequest(
        Consumer<Object,OpMessage> flushConsumer,
        TopicPartition flushPart,
        long firstFlushReqOffset
    ) {
        if (firstFlushReqOffset == 0)
            return null; // We are at the beginning, no previous committed flush requests.

        long currentPosition = flushConsumer.position(flushPart);
        assert currentPosition >= firstFlushReqOffset + 1: currentPosition + " " + firstFlushReqOffset;

        long startOffset = firstFlushReqOffset - 1; // Take the previous record.
        flushConsumer.seek(flushPart, startOffset);

        ConsumerRecord<Object,OpMessage> max = null;

        do {
            List<ConsumerRecord<Object,OpMessage>> flushRecs = flushConsumer.poll(seconds(1)).records(flushPart);

            if (flushRecs.isEmpty())
                continue;

            max = flushRecs.get(0);
        }
        while (flushConsumer.position(flushPart) < firstFlushReqOffset);

        log.debug("Found max history flush request for partition {}: {}", flushPart, max);

        if (max == null) {
            throw new ReplicaMapException("Committed flush requests lost before offset: " + firstFlushReqOffset +
                ", check flush topic retention settings: " + flushTopic);
        }

        // Sometimes this happens because Kafka does not fetch correctly the previous record for some reason.
        // This is not a critical error, we just reset and try again.
        if (max.offset() >= firstFlushReqOffset) {
            throw new ReplicaMapException("Committed flush requests lost before offset: " + firstFlushReqOffset +
                ", found record: " + max);
        }

        flushConsumer.seek(flushPart, currentPosition);

        return max;
    }

    protected boolean flushPartition(
        Consumer<Object,OpMessage> flushConsumer,
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
            flushOffsetData = flushTx(dataProducer, dataBatch, flushPart, flushConsumerOffset);
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
        OffsetAndMetadata flushConsumerOffset
    ) throws ExecutionException, InterruptedException {
        int part = flushPart.partition();
        Future<RecordMetadata> lastDataRecMetaFut = null;

        Map<Object,Future<RecordMetadata>> futs = new HashMap<>();

        dataProducer.beginTransaction();
        for (Map.Entry<Object,Object> entry : dataBatch.entrySet()) {
            lastDataRecMetaFut = dataProducer.send(new ProducerRecord<>(
                dataTopic, part, entry.getKey(), entry.getValue()));

            futs.put(entry.getKey(), lastDataRecMetaFut);
        }
        dataProducer.sendOffsetsToTransaction(singletonMap(flushPart, flushConsumerOffset), flushConsumerGroupId);
        dataProducer.commitTransaction();

        for (Map.Entry<Object,Object> entry : dataBatch.entrySet())
            trace.trace("flushTx {} minOffset={}, maxOffset={}, maxCleanOffset={}, dataOffset={}, key={}, val={}",
                flushPart, dataBatch.getMinOffset(), dataBatch.getMaxOffset(), dataBatch.getMaxCleanOffset(),
                futs.get(entry.getKey()).get().offset(), entry.getKey(), entry.getValue());

        // We check that the batch is not empty, thus we have to have the last record non-null here.
        // Since ReplicaMap checks preconditions locally before sending anything to Kafka,
        // it is impossible to have a large number of failed update attempts,
        // there must always be a winner, so we will be able to commit periodically.
        assert lastDataRecMetaFut != null;
        return lastDataRecMetaFut.get().offset();
    }

    protected void sendFlushNotification(TopicPartition dataPart, long flushOffsetData, long flushOffsetOps) {
        ProducerRecord<Object,OpMessage> flushNotification = new ProducerRecord<>(opsTopic, dataPart.partition(), null,
            newOpMessage(flushOffsetData, flushOffsetOps));

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

    protected OpMessage newOpMessage(long flushOffsetData, long flushOffsetOps) {
        // lastCleanOffsetOps here is the same as flushOffsetOps
        return new OpMessage(OP_FLUSH_NOTIFICATION, clientId, flushOffsetData, flushOffsetOps, 0L);
    }

    protected Producer<Object,Object> newDataProducer(int part) {
        Producer<Object,Object> p = dataProducerFactory.apply(part);
        p.initTransactions();
        return p;
    }

    protected Consumer<Object,OpMessage> newFlushConsumer(int ignore) {
        Consumer<Object,OpMessage> c = flushConsumerFactory.get();
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
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty())
                return;

            log.debug("Flush partitions revoked: {}", partitions);
            clearUnprocessedFlushRequests(partitions);
            resetDataProducers(partitions);
        }
    }
}
