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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Processes flush requests (flushes the collected updates to the `data` topic)
 * and clean requests (cleans the flush queue).
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class FlushWorker extends Worker {
    private static final Logger log = LoggerFactory.getLogger(FlushWorker.class);

    protected static final Comparator<ConsumerRecord<Object,OpMessage>> FLUSH_OFFSET_OPS_CMP =
        Comparator.comparingLong(rec -> rec.value().getFlushOffsetOps());
    protected static final Comparator<ConsumerRecord<?,?>> OFFSET_CMP =
        Comparator.comparingLong(ConsumerRecord::offset);

    protected final long clientId;

    protected final String dataTopic;
    protected final String opsTopic;
    protected final String flushTopic;

    protected final String flushConsumerGroupId;

    protected final int flushMinOps;
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

    protected final Map<TopicPartition, OpMessage> maxFlushRequests = new HashMap<>();

    public FlushWorker(
        long clientId,
        String dataTopic,
        String opsTopic,
        String flushTopic,
        int workerId,
        String flushConsumerGroupId,
        int flushMinOps,
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
        this.flushMinOps = flushMinOps;
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
        Utils.wakeup(() -> dataConsumers.get(workerId, null));

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

    protected boolean awaitOpsSteady(long pollTimeoutMs) throws ExecutionException, InterruptedException {
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
        if (!awaitOpsSteady(pollTimeoutMs))
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
        }
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Failed to poll flush consumer for topic " + flushTopic, e);

            flushConsumers.reset(workerId, flushConsumer);
            return false;
        }

        if (recs.isEmpty())
            return false;

        boolean flushed = false;

        for (TopicPartition flushPart : recs.partitions())
             flushed |= flushPartition(flushConsumer, flushPart, recs.records(flushPart));

        return flushed;
    }

    protected boolean isTooSmallBatch(int batchSize, int collectedAll) {
        assert 0 <= batchSize && batchSize <= collectedAll;
        return batchSize == 0 || collectedAll < flushMinOps;
    }

    protected boolean isMaxFlushOffset(
        Consumer<Object,OpMessage> flushConsumer,
        TopicPartition flushPart,
        ConsumerRecord<Object,OpMessage> flushRequest
    ) {
        OpMessage flushOp = flushRequest.value();
        OpMessage maxOp = maxFlushRequests.get(flushPart);

        if (maxOp == null) {
            try {
                maxOp = loadFlushHistoryMax(flushConsumer, flushPart, flushRequest);
            }
            catch (Exception e) {
                if (!Utils.isInterrupted(e))
                    log.error("Failed to load history for partition: " + flushPart, e);

                flushConsumers.reset(workerId, flushConsumer);
                return false;
            }

            if (maxOp == null) {
                maxFlushRequests.put(flushPart, flushOp);
                return true;
            }
            maxFlushRequests.put(flushPart, maxOp);
        }

        if (maxOp.getFlushOffsetOps() >= flushOp.getFlushOffsetOps())
            return false;

        maxFlushRequests.put(flushPart, flushOp);
        return true;
    }

    protected OpMessage loadFlushHistoryMax(
        Consumer<Object,OpMessage> flushConsumer,
        TopicPartition flushPart,
        ConsumerRecord<Object,OpMessage> req
    ) {
        long currentPosition = flushConsumer.position(flushPart);
        assert currentPosition >= req.offset() + 1;
        long startOffset = currentPosition - historyFlushRecords;

        if (startOffset < 0)
            startOffset = 0;

        flushConsumer.seek(flushPart, startOffset);

        ConsumerRecord<Object,OpMessage> max = null;

        while (flushConsumer.position(flushPart) < req.offset()) {
            List<ConsumerRecord<Object,OpMessage>> flushRecs =
                flushConsumer.poll(seconds(1)).records(flushPart);

            if (flushRecs.isEmpty())
                continue;

            flushRecs = flushRecs.stream()
                .filter(r -> OFFSET_CMP.compare(r, req) < 0)
                .collect(Collectors.toList());

            if (flushRecs.isEmpty())
                break;

            ConsumerRecord<Object,OpMessage> newMax = findMax(flushRecs, FLUSH_OFFSET_OPS_CMP);
            if (max == null || FLUSH_OFFSET_OPS_CMP.compare(max, newMax) < 0)
                max = newMax;
        }

        flushConsumer.seek(flushPart, currentPosition);

        return max == null ? null : max.value();
    }

    protected boolean flushPartition(
        Consumer<Object,OpMessage> flushConsumer,
        TopicPartition flushPart,
        List<ConsumerRecord<Object,OpMessage>> flushRecs
    ) {
        // Here we must not modify any local state until transaction is successfully committed.
        int part = flushPart.partition();
        TopicPartition dataPart = new TopicPartition(dataTopic, part);

        ConsumerRecord<Object,OpMessage> flushRequest = findMax(flushRecs, FLUSH_OFFSET_OPS_CMP);

        if (!isMaxFlushOffset(flushConsumer, flushPart, flushRequest))
            return false; // ignore racy flush request

        log.debug("Processing flush request {} for partition {}", flushRequest, dataPart);

        FlushQueue flushQueue = flushQueues.get(part);

        long cleanOffsetOps = flushRequest.value().getCleanOffsetOps();
        if (cleanOffsetOps >= 0)
            flushQueue.clean(cleanOffsetOps);

        FlushQueue.Batch dataBatch = flushQueue.collectUpdateRecords();
        int dataBatchSize = dataBatch.size();
        int collectedAll = dataBatch.getCollectedAll();

        if (log.isDebugEnabled()) {
            log.debug("Collected batch for partition {}, dataBatchSize: {}, collectedAll: {}",
                dataPart, dataBatchSize, collectedAll);
        }

        if (isTooSmallBatch(dataBatchSize, collectedAll)) {
            if (log.isDebugEnabled()) {
                log.debug("Too small batch for partition {}, dataBatchSize: {}, collectedAll: {}",
                     dataPart, dataBatchSize, collectedAll);
            }

            if (dataBatchSize == 0) // Looks like we are far behind, let someone else to handle flushes.
                flushConsumers.reset(workerId, flushConsumer); // Initiate partition rebalance.

            return false; // The next flush will take more records, this is ok.
        }

        Producer<Object,Object> dataProducer;
        try {
            dataProducer = dataProducers.get(part, this::newDataProducer);
        }
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Failed to create data producer for partition " + dataPart, e);

            flushConsumers.reset(workerId, flushConsumer); // Initiate partition rebalance.
            return false; // The next flush will be our retry.
        }

        Consumer<Object,Object> dataConsumer = null;
        long flushOffsetData = -1;
        OffsetAndMetadata flushConsumerOffset = null;

        try {
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

            // The following check is needed to make sure that there is no race
            // between this thread and any other flusher. Otherwise the following scenario may happen:
            // we've read flush-request X, got to sleep, someone else reads flush-requests X and Y
            // and successfully flushes them, then we wakeup, fence the previous guy and flush X once more.
            // This flush reordering may break the state.
            // The check just before committing the TX makes sure that either we are still own the partition
            // or the other guy will fence us.
            // This only protects from reordering but does not protect from "double flush".
            checkConsumerOwnsPartition(flushConsumer, flushPart);

            dataProducer.commitTransaction();
            log.trace("TX committed for partition {}", dataPart);

            // In isTooSmallBatch method we check that the batch is not empty,
            // thus we have to have the last record non-null here.
            // Since ReplicaMap checks preconditions locally before sending anything to Kafka,
            // it is impossible to have a large number of failed update attempts,
            // there must always be a winner, so we will be able to commit periodically.
            assert lastDataRecMetaFut != null;

            flushOffsetData = lastDataRecMetaFut.get().offset();

            if (log.isTraceEnabled())
                log.trace("Committed flush offset for partition {} is {}", dataPart, flushOffsetData);

            if (dataConsumer != null)
                readBackAndCheckCommittedRecords(dataConsumer, dataPart, dataBatch, flushOffsetData);
        }
        catch (Exception e) {
            if (Utils.isInterrupted(e) || e instanceof ProducerFencedException || e instanceof ReplicaMapException) {
                log.warn("Failed to flush data for partition {}, flushOffsetData: {}, flushConsumerOffset: {}, " +
                        "flushQueueSize: {}, reason: {}", dataPart, flushOffsetData, flushConsumerOffset,
                        flushQueue.size(), Utils.getMessage(e));
            }
            else
                log.error("Failed to flush data for partition " + dataPart + ", exception:", e);

            dataProducers.reset(part, dataProducer); // Producer may be broken, needs to be recreated on the next flush.
            flushConsumers.reset(workerId, flushConsumer); // Initiate partition rebalance: maybe other flusher will be luckier.

            if (dataConsumer != null)
                dataConsumers.reset(workerId, dataConsumer);

            return false; // No other handling, the next flush will be our retry.
        }

        long flushOffsetOps = dataBatch.getMaxOffset();

        if (flushQueue.clean(flushOffsetOps) > 0) {
            sendFlushNotification(part, flushOffsetData, flushOffsetOps);

            if (log.isDebugEnabled()) {
                log.debug("Successfully flushed {} data records for partition {}, flushOffsetOps: {}, flushOffsetData: {}, flushQueueSize: {}",
                    dataBatchSize, dataPart, flushOffsetOps, flushOffsetData, flushQueue.size());
            }
        }

        return true;
    }

    protected void checkConsumerOwnsPartition(Consumer<?,?> consumer, TopicPartition part) {
        // FIXME probably does not work as expected, need some way
        if (!consumer.assignment().contains(part))
            throw new ReplicaMapException("No longer own the partition: " + part);
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

    protected void clearMaxFlushRequests(Collection<TopicPartition> partitions) {
        for (TopicPartition part : partitions)
            maxFlushRequests.remove(part);
    }

    /**
     * We need to clean the cached max flush request here and reload the history
     * when the partition will be ours again because otherwise we may miss
     * flush requests with greater offsets that were processed by other clients.
     */
    protected class MaxFlushRequestsCleaner implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.debug("Flush partitions revoked: {}", partitions);
            clearMaxFlushRequests(partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.debug("Flush partitions assigned: {}", partitions);
            clearMaxFlushRequests(partitions);
        }
    }
}
