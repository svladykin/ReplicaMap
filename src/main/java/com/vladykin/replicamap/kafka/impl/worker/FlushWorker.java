package com.vladykin.replicamap.kafka.impl.worker;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.util.FlushQueue;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vladykin.replicamap.base.ReplicaMapBase.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.kafka.impl.util.Utils.findMax;
import static com.vladykin.replicamap.kafka.impl.util.Utils.millis;
import static com.vladykin.replicamap.kafka.impl.util.Utils.seconds;
import static com.vladykin.replicamap.kafka.impl.worker.OpsWorker.LOAD_FLUSH_LOG;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.Comparator.comparingLong;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Processes flush requests (flushes the collected updates to the `data` topic)
 * and clean requests (cleans the flush queue).
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class FlushWorker extends Worker {
    private static final Logger loadFlushLog = LoggerFactory.getLogger(LOAD_FLUSH_LOG);
    private static final Logger log = LoggerFactory.getLogger(FlushWorker.class);

    protected final long clientId;

    protected final String dataTopic;
    protected final String opsTopic;
    protected final String flushTopic;

    protected final String flushConsumerGroupId;
    protected final int historyFlushRecords;

    protected final LazyList<Consumer<Object,OpMessage>> flushConsumers;
    protected final Supplier<Consumer<Object,OpMessage>> flushConsumerFactory;

    protected final LazyList<Producer<Object,Object>> dataProducers;
    protected final IntFunction<Producer<Object,Object>> dataProducerFactory;

    protected final LazyList<Consumer<Object,Object>> dataConsumers;
    protected final Supplier<Consumer<Object,Object>> dataConsumerFactory;

    protected final Producer<Object,OpMessage> opsProducer;

    protected final Queue<ConsumerRecord<Object,OpMessage>> cleanQueue;
    protected final List<FlushQueue> flushQueues;

    protected final CompletableFuture<ReplicaMapManager> opsSteadyFut;

    protected final long maxPollTimeout;
    protected final long readBackTimeout;

    protected final Map<TopicPartition,TreeSet<OpMessage>> unprocessedFlushRequests = new HashMap<>();

    public FlushWorker(
        long clientId,
        String dataTopic,
        String opsTopic,
        String flushTopic,
        int workerId,
        String flushConsumerGroupId,
        int historyFlushRecords,
        LazyList<Producer<Object,Object>> dataProducers,
        Producer<Object,OpMessage> opsProducer,
        List<FlushQueue> flushQueues,
        Queue<ConsumerRecord<Object,OpMessage>> cleanQueue,
        CompletableFuture<ReplicaMapManager> opsSteadyFut,
        long maxPollTimeout,
        long readBackTimeout,
        IntFunction<Producer<Object,Object>> dataProducerFactory,
        LazyList<Consumer<Object,OpMessage>> flushConsumers,
        Supplier<Consumer<Object,OpMessage>> flushConsumerFactory,
        LazyList<Consumer<Object,Object>> dataConsumers,
        Supplier<Consumer<Object,Object>> dataConsumerFactory
    ) {
        super("replicamap-flush-" + dataTopic + "-" +
            Long.toHexString(clientId), workerId);

        this.clientId = clientId;
        this.dataTopic = dataTopic;
        this.opsTopic = opsTopic;
        this.flushTopic = flushTopic;
        this.flushConsumerGroupId = flushConsumerGroupId;
        this.historyFlushRecords = historyFlushRecords;
        this.dataProducers = dataProducers;
        this.opsProducer = opsProducer;
        this.flushQueues = flushQueues;
        this.cleanQueue = cleanQueue;
        this.opsSteadyFut = opsSteadyFut;
        this.maxPollTimeout = maxPollTimeout;
        this.readBackTimeout = readBackTimeout;
        this.flushConsumers = flushConsumers;
        this.dataProducerFactory = dataProducerFactory;
        this.flushConsumerFactory = flushConsumerFactory;
        this.dataConsumers = dataConsumers;
        this.dataConsumerFactory = dataConsumerFactory;
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
        Utils.wakeup(() -> flushConsumers.get(workerId, null));
        Utils.wakeup(() -> dataConsumers == null ? null : dataConsumers.get(workerId, null));

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
            long cleanedCnt = flushQueue.clean(flushOffsetOps);

            if (log.isDebugEnabled()) {
                log.debug("Processed clean request {} for partition {}, flushQueueSize: {}, cleanedCnt: {}",
                    cleanRequest, new TopicPartition(cleanRequest.topic(), cleanRequest.partition()), cleanedCnt,
                    flushQueue.size());
            }
        }
    }

    protected void clearUnprocessedFlushRequestsUntil(TopicPartition flushPart, long maxOffsetOps) {
        TreeSet<OpMessage> flushRequests = unprocessedFlushRequests.get(flushPart);

        if (flushRequests == null)
            return;

        Iterator<OpMessage> iter = flushRequests.iterator();

        while (iter.hasNext()) {
            OpMessage request = iter.next();

            if (request.getFlushOffsetOps() > maxOffsetOps)
                break;

            iter.remove();
        }

        // Do not remove empty flushRequests from unprocessedFlushRequests,
        // otherwise we will reload flush history each time.
    }

    protected boolean awaitOpsWorkersSteady(long pollTimeoutMs) throws ExecutionException, InterruptedException {
        try {
            opsSteadyFut.get(pollTimeoutMs, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
            return false;
        }
        return true;
    }

    protected boolean processFlushRequests(long pollTimeoutMs) throws InterruptedException, ExecutionException {
        // We start consuming flush requests only when the ops workers are steady,
        // because otherwise we may have not enough data to flush.
        if (!awaitOpsWorkersSteady(pollTimeoutMs))
            return false;

        Consumer<Object,OpMessage> flushConsumer;
        try {
            flushConsumer = flushConsumers.get(workerId, this::newFlushConsumer);
        }
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Failed to create flush consumer for topic " + flushTopic, e);

            return false;
        }

        ConsumerRecords<Object,OpMessage> recs;
        try {
            recs = flushConsumer.poll(millis(pollTimeoutMs));

            // Add new records to unprocessed set and load history if it is the first flush for the partition.
            for (TopicPartition flushPart : recs.partitions()) {
                List<ConsumerRecord<Object,OpMessage>> partRecs = recs.records(flushPart);

                TreeSet<OpMessage> flushRequests = unprocessedFlushRequests.get(flushPart);

                // If it is the first batch of records for this partition we need to initialize the processing for it.
                if (flushRequests == null) {
                    // Load flush history and clean the flushQueue until the max historical offset,
                    // after that even if the OpsWorker is behind the flushQueue will not accept outdated records.
                    // We do not need to keep the history and can safely assume that no double flushes will happen.
                    OpMessage maxHistoryReq = loadFlushHistoryMax(flushConsumer, flushPart, partRecs.get(0).offset());
                    if (maxHistoryReq != null)
                        flushQueues.get(flushPart.partition()).clean(maxHistoryReq.getFlushOffsetOps());

                    flushRequests = initUnprocessedFlushRequests(flushPart);
                }

                for (ConsumerRecord<Object,OpMessage> rec : partRecs)
                    flushRequests.add(rec.value());
            }
        }
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Failed to poll flush consumer for topic " + flushTopic, e);

            resetAll(flushConsumer, null);
            return false;
        }

        boolean flushed = false;

        // Try process the unprocessed set.
        for (Map.Entry<TopicPartition,TreeSet<OpMessage>> entry : unprocessedFlushRequests.entrySet())
             flushed |= flushPartition(flushConsumer, entry.getKey(), entry.getValue());

        return flushed;
    }

    protected void resetAll(Consumer<Object,OpMessage> flushConsumer, Consumer<Object,Object> dataConsumer) {
        unprocessedFlushRequests.clear();

        if (dataConsumer != null)
            dataConsumers.reset(workerId, dataConsumer);

        for (int i = 0; i < dataProducers.size(); i++)
            resetDataProducer(new TopicPartition(flushTopic, i));

        flushConsumers.reset(workerId, flushConsumer);
    }

    protected TreeSet<OpMessage> initUnprocessedFlushRequests(TopicPartition flushPart) {
        TreeSet<OpMessage> flushRequests = new TreeSet<>(comparingLong(OpMessage::getFlushOffsetOps));

        if (unprocessedFlushRequests.put(flushPart, flushRequests) != null)
            throw new IllegalStateException("Flush requests already initialized for partition: " + flushPart);

        return flushRequests;
    }

    protected OpMessage loadFlushHistoryMax(
        Consumer<Object,OpMessage> flushConsumer,
        TopicPartition flushPart,
        long maxOffset
    ) {
        long currentPosition = flushConsumer.position(flushPart);
        assert currentPosition >= maxOffset + 1: currentPosition + " " + maxOffset;

        long startOffset = maxOffset - historyFlushRecords;
        if (startOffset < 0)
            startOffset = 0;
        flushConsumer.seek(flushPart, startOffset);

        Comparator<ConsumerRecord<Object,OpMessage>> cmpFlushOffsetOps =
            comparingLong(r -> r.value().getFlushOffsetOps());

        ConsumerRecord<Object,OpMessage> max = null;

        while (flushConsumer.position(flushPart) < maxOffset) {
            List<ConsumerRecord<Object,OpMessage>> flushRecs = flushConsumer.poll(seconds(1))
                .records(flushPart);

            if (flushRecs.isEmpty())
                continue;

            flushRecs = flushRecs.stream()
                .filter(r -> r.offset() < maxOffset)
                .collect(toList());

            if (flushRecs.isEmpty())
                break;

            ConsumerRecord<Object,OpMessage> nextMax = findMax(flushRecs, cmpFlushOffsetOps);

            if (max == null || cmpFlushOffsetOps.compare(max, nextMax) < 0)
                max = nextMax;
        }

        log.debug("Found max history flush request {} for partition {}", max, flushPart);

        flushConsumer.seek(flushPart, currentPosition);
        return max == null ? null : max.value();
    }

    protected boolean flushPartition(
        Consumer<Object,OpMessage> flushConsumer,
        TopicPartition flushPart,
        TreeSet<OpMessage> flushRequests
    ) {
        if (flushRequests.isEmpty())
            return false;

        // Here we must not modify any local state until transaction is successfully committed.
        int part = flushPart.partition();
        TopicPartition dataPart = new TopicPartition(dataTopic, part);

        log.debug("Processing flush requests {} for partition {}", flushRequests, dataPart);

        FlushQueue flushQueue = flushQueues.get(part);

        flushQueue.clean(flushRequests.stream()
            .mapToLong(OpMessage::getCleanOffsetOps).max().getAsLong());

        FlushQueue.Batch dataBatch = flushQueue.collect(flushRequests.stream()
            .mapToLong(OpMessage::getFlushOffsetOps));

        if (dataBatch == null) {
            // Check if we are too far behind.
            if (flushRequests.size() > 1) // TODO add to config
                resetAll(flushConsumer, null);

            return false; // No enough data.
        }

        int dataBatchSize = dataBatch.size();
        int collectedAll = dataBatch.getCollectedAll();

        if (log.isDebugEnabled()) {
            log.debug("Collected batch for partition {}, dataBatchSize: {}, collectedAll: {}",
                dataPart, dataBatchSize, collectedAll);
        }

        Producer<Object,Object> dataProducer;
        Consumer<Object,Object> dataConsumer = null;
        long flushOffsetData = -1;
        OffsetAndMetadata flushConsumerOffset = null;

        try {
            dataProducer = dataProducers.get(part, null);
            log.trace("Data producer for flushing partition {}: {}", dataPart, dataProducer);

            if (dataConsumers != null) {
                dataConsumer = dataConsumers.get(workerId, this::newDataConsumer);
                Set<TopicPartition> dataPartSet = singleton(dataPart);
                dataConsumer.assign(dataPartSet);
                dataConsumer.seekToEnd(dataPartSet);

                // The next line is needed to make sure that we actually have fetched the current end position,
                // because all the previous calls can go lazy and just do nothing and on poll() we will get to
                // the wrong position.
                long dataConsumerOffset = dataConsumer.position(dataPart);

                if (log.isTraceEnabled())
                    log.trace("Data consumer initialized for partition {} at offset: {}", dataPart, dataConsumerOffset);
            }

            dataProducer.beginTransaction();
            log.trace("TX started for partition {}", dataPart);

            Future<RecordMetadata> lastDataRecMetaFut = null;
            for (Map.Entry<Object,Object> entry : dataBatch.entrySet()) {
                lastDataRecMetaFut = dataProducer.send(new ProducerRecord<>(
                    dataTopic, part, entry.getKey(), entry.getValue()));
            }

            if (log.isTraceEnabled())
                log.trace("Sent {} entries to TX for partition {}", dataBatchSize, dataPart);

            flushConsumerOffset = new OffsetAndMetadata(flushConsumer.position(flushPart));
            dataProducer.sendOffsetsToTransaction(
                singletonMap(flushPart, flushConsumerOffset), flushConsumerGroupId);

            log.trace("Sent {} offset to TX for partition {}", flushConsumerOffset, flushPart);

            dataProducer.commitTransaction();
            log.trace("TX committed for partition {}", dataPart);

            // In isTooSmallBatch method we check that the batch is not empty,
            // thus we have to have the last record non-null here.
            // Since ReplicaMap checks preconditions locally before sending anything to Kafka,
            // it is impossible to have a large number of failed update attempts,
            // there must always be a winner, so we will be able to commit periodically.
            assert lastDataRecMetaFut != null;

            flushOffsetData = lastDataRecMetaFut.get().offset();

            if (log.isDebugEnabled()) {
                log.debug("Committed flush offset for data partition {} is {}, dataProducer: {}, dataBatch: {}",
                    dataPart, flushOffsetData, dataProducer, dataBatch);
            }

            if (loadFlushLog.isTraceEnabled()) {
                loadFlushLog.trace("Committed flush offset for data partition {} is {}, dataProducer: {}, dataBatch: {}",
                    dataPart, flushOffsetData, dataProducer, dataBatch);
            }

            if (dataConsumer != null)
                readBackAndCheckCommittedRecords(dataConsumer, dataPart, dataBatch, flushOffsetData);
        }
        catch (ProducerFencedException e) {
            log.warn("Fenced while flushing data for partition {}, flushQueueSize: {}", dataPart, flushQueue.size());
            resetDataProducer(flushPart);
            return false;
        }
        catch (Exception e) {
            if (Utils.isInterrupted(e) || e instanceof ReplicaMapException) {
                log.warn("Failed to flush data for partition {}, flushOffsetData: {}, flushConsumerOffset: {}, " +
                        "flushQueueSize: {}, reason: {}", dataPart, flushOffsetData, flushConsumerOffset,
                        flushQueue.size(), Utils.getMessage(e));
            }
            else
                log.error("Failed to flush data for partition " + dataPart + ", exception:", e);

            resetAll(flushConsumer, dataConsumer);
            return false; // No other handling, the next flush will be our retry.
        }

        long flushOffsetOps = dataBatch.getMaxOffset();
        clearUnprocessedFlushRequestsUntil(flushPart, flushOffsetOps);

        if (flushQueue.clean(flushOffsetOps) > 0) {
            sendFlushNotification(part, flushOffsetData, flushOffsetOps);

            if (log.isDebugEnabled()) {
                log.debug("Successfully flushed {} data records for partition {}, flushOffsetOps: {}" +
                        ", flushOffsetData: {}, flushQueueSize: {}",
                    dataBatchSize, dataPart, flushOffsetOps, flushOffsetData, flushQueue.size());
            }
        }

        return true;
    }

    protected void readBackAndCheckCommittedRecords(
        Consumer<Object,Object> dataConsumer,
        TopicPartition dataPart,
        Map<Object,Object> dataBatch,
        long flushOffsetData
    ) {
        long start = System.nanoTime();
        int readRecords = 0;
        int initBatchSize = dataBatch.size();

        for(;;) {
            ConsumerRecords<Object,Object> data = dataConsumer.poll(seconds(1));
            ConsumerRecord<Object,Object> lastSeenRec = null;

            for (ConsumerRecord<Object,Object> rec : data.records(dataPart)) {
                lastSeenRec = rec;

                if (rec.offset() > flushOffsetData)
                    break;

                if (!dataBatch.remove(rec.key(), rec.value()))
                    throw new ReplicaMapException("Record is not found in the committed batch: " + rec);

                if (dataBatch.isEmpty())
                    break;
            }

            if (dataBatch.isEmpty()) {
                assert lastSeenRec != null;

                if (lastSeenRec.offset() != flushOffsetData)
                    throw new ReplicaMapException("Committed data offset mismatch: " + flushOffsetData + " vs " + lastSeenRec.offset());

                return; // All records were found.
            }

            if (lastSeenRec != null && lastSeenRec.offset() > flushOffsetData) {
                throw new ReplicaMapException(
                    "Committed data offset reached, but not all the committed records found" +
                        ", readRecords: " + readRecords + ", initBatchSize: " + initBatchSize +
                        ", endBatchSize: " + dataBatch.size());
            }

            long time = NANOSECONDS.toMillis(System.nanoTime() - start);

            if (time >= readBackTimeout) {
                throw new ReplicaMapException("Failed after " + time + "ms for partition " + dataPart +
                    ", readRecords: " + readRecords + ", initBatchSize: " + initBatchSize +
                    ", endBatchSize: " + dataBatch.size());
            }
        }
    }

    protected void sendFlushNotification(int part, long flushOffsetData, long flushOffsetOps) {
        ProducerRecord<Object,OpMessage> flushNotification = new ProducerRecord<>(opsTopic, part, null,
            newOpMessage(flushOffsetData, flushOffsetOps));

        log.debug("Sending flush notification: {}", flushNotification);

        try {
            opsProducer.send(flushNotification);
        }
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Failed to send flush notification: " + flushNotification, e);
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
        c.subscribe(singleton(flushTopic), new MaxFlushRequestsCleaner());
        return c;
    }

    protected Consumer<Object,Object> newDataConsumer(int ignore) {
        return dataConsumerFactory.get();
    }

    protected void clearUnprocessedFlushRequests(Collection<TopicPartition> flushPartitions) {
        log.debug("Clearing unprocessed flush requests for partitions: {}", flushPartitions);
        for (TopicPartition part : flushPartitions)
            unprocessedFlushRequests.remove(part);
    }

    protected void initDataProducers(Collection<TopicPartition> flushPartitions) {
        if (log.isDebugEnabled()) {
            log.debug("Initializing data producers for partitions: {}", flushPartitions.stream()
                .map(p -> new TopicPartition(dataTopic, p.partition())).collect(toList()));
        }

        for (TopicPartition partition : flushPartitions) {
            int part = partition.partition();

            if (dataProducers.get(part, null) != null)
                throw new IllegalStateException("Producer exists for part: " + part);

            dataProducers.get(part, this::newDataProducer);
        }
    }

    protected void resetDataProducers(Collection<TopicPartition> flushPartitions) {
        if (log.isDebugEnabled()) {
            log.debug("Resetting data producers for partitions: {}", flushPartitions.stream()
                .map(p -> new TopicPartition(dataTopic, p.partition())).collect(toList()));
        }

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
    protected class MaxFlushRequestsCleaner implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.debug("Flush partitions assigned: {}", partitions);
            clearUnprocessedFlushRequests(partitions);
            initDataProducers(partitions);
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.debug("Flush partitions revoked: {}", partitions);
            clearUnprocessedFlushRequests(partitions);
            resetDataProducers(partitions);
        }
    }
}
