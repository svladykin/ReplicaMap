package com.vladykin.replicamap.kafka.impl.worker.ops;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.msg.FlushNotification;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import com.vladykin.replicamap.kafka.impl.msg.MapUpdate;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import com.vladykin.replicamap.kafka.impl.worker.Worker;
import com.vladykin.replicamap.kafka.impl.worker.flush.FlushQueue;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_PUT;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_REMOVE_ANY;
import static com.vladykin.replicamap.kafka.impl.util.Utils.MIN_POLL_TIMEOUT;
import static com.vladykin.replicamap.kafka.impl.worker.flush.FlushWorker.OPS_OFFSET_HEADER;
import static java.util.Collections.singleton;

/**
 * Loads the initial state from the `data` topic and then
 * polls the `ops` topic and applies the updates to the inner map.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class OpsWorker extends Worker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(OpsWorker.class);

    protected final UUID clientId;

    protected final String dataTopic;
    protected final String opsTopic;
    protected final String flushTopic;

    protected final List<TopicPartition> assignedOpsParts;

    protected Consumer<Object,Object> dataConsumer;
    protected final Consumer<Object,OpMessage> opsConsumer;
    protected final Producer<Object,FlushRequest> flushProducer;

    protected final int flushPeriodOps;
    protected final List<FlushQueue> flushQueues;

    protected final OpsUpdateHandler updateHandler;

    protected final CompletableFuture<Void> steadyFut = new CompletableFuture<>();
    protected Map<TopicPartition,Long> endOffsetsOps;
    protected int maxAllowedSteadyLag;

    protected final LongAdder sentFlushRequests;
    protected final LongAdder receivedUpdates;
    protected final LongAdder receivedDataRecords;
    protected final LongAdder receivedFlushNotifications;

    public OpsWorker(
        UUID clientId,
        String dataTopic,
        String opsTopic,
        String flushTopic,
        int workerId,
        Set<Integer> assignedParts,
        Consumer<Object,Object> dataConsumer,
        Consumer<Object,OpMessage> opsConsumer,
        Producer<Object,FlushRequest> flushProducer,
        int flushPeriodOps,
        List<FlushQueue> flushQueues,
        OpsUpdateHandler updateHandler,
        LongAdder sentFlushRequests,
        LongAdder receivedUpdates,
        LongAdder receivedDataRecords,
        LongAdder receivedFlushNotifications
    ) {
        super("replicamap-ops-" + dataTopic + "-" + workerId + "-" + clientId);

        if (assignedParts == null || assignedParts.isEmpty())
            throw new ReplicaMapException("Ops worker " + workerId + " received 0 partitions.");

        assignedOpsParts = assignedParts.stream()
            .map((part) -> new TopicPartition(opsTopic, part))
            .collect(Collectors.toList());

        this.clientId = clientId;
        this.dataTopic = dataTopic;
        this.opsTopic = opsTopic;
        this.flushTopic = flushTopic;
        this.dataConsumer = dataConsumer;
        this.opsConsumer = opsConsumer;
        this.flushProducer = flushProducer;
        this.flushPeriodOps = flushPeriodOps;
        this.flushQueues = flushQueues;
        this.updateHandler = updateHandler;
        this.sentFlushRequests = sentFlushRequests;
        this.receivedUpdates = receivedUpdates;
        this.receivedDataRecords = receivedDataRecords;
        this.receivedFlushNotifications = receivedFlushNotifications;
    }

    protected Map<TopicPartition, Long> loadData() {
        try {
            Map<TopicPartition,Long> opsOffsets = new HashMap<>();

            for (TopicPartition opsPart : assignedOpsParts) {
                TopicPartition dataPart = new TopicPartition(dataTopic, opsPart.partition());
                log.debug("Loading data for partition {}", dataPart);

                long flushOffsetOps = loadDataForPartition(dataPart) + 1; // Add 1 because we need first unflushed position.
                opsOffsets.put(opsPart, flushOffsetOps);
                checkInterrupted();
            }

            return opsOffsets;
        }
        finally {
            Utils.close(dataConsumer); // We do not need it anymore.
            dataConsumer = null;
        }
    }

    protected long readOpsOffsetHeader(ConsumerRecord<Object,Object> dataRec) {
        Header header = dataRec.headers().lastHeader(OPS_OFFSET_HEADER);

        if (header == null)
            throw new ReplicaMapException("No '" + OPS_OFFSET_HEADER +
                "' header found for the last committed data record: " + dataRec);

        return Utils.deserializeVarlong(header.value());
    }

    protected long loadDataForPartition(TopicPartition dataPart) {
        dataConsumer.assign(singleton(dataPart));
        dataConsumer.seekToBeginning(singleton(dataPart));

        ConsumerRecord<Object,Object> lastDataRec = null;

        for (;;) {
            ConsumerRecords<Object,Object> recs = Utils.poll(dataConsumer, MIN_POLL_TIMEOUT);

            if (recs.isEmpty()) {
                // With read_committed endOffset means LSO.
                if (dataConsumer.position(dataPart) < Utils.endOffset(dataConsumer, dataPart))
                    continue;

                return lastDataRec == null ? -1L : readOpsOffsetHeader(lastDataRec);
            }

            for (ConsumerRecord<Object,Object> rec : recs.records(dataPart)) {
                log.trace("Loading data partition {}, record: {}", dataPart, rec);
                applyDataTopicRecord(rec);
                lastDataRec = rec;
            }
        }
    }

    protected void applyDataTopicRecord(ConsumerRecord<Object,Object> dataRec) {
        Object key = dataRec.key();
        Object val = dataRec.value();
        byte opType = val == null ? OP_REMOVE_ANY : OP_PUT;

        UUID zero = new UUID(0,0);

        receivedDataRecords.increment();
        updateHandler.applyReceivedUpdate(dataRec.topic(), dataRec.partition(), dataRec.offset(),
            zero, 0L, opType, key, null, val, null, null);
    }

    protected void applyOpsTopicRecords(TopicPartition opsPart, List<ConsumerRecord<Object,OpMessage>> partRecs) {
        var flushQueue = flushQueues.get(opsPart.partition());
        var updatedValue = new UpdatedValue();

        for (var rec : partRecs) {
            updatedValue.clear();

            if (log.isTraceEnabled())
                log.trace("Applying op to partition {}, steady: {}, record: {}", opsPart, isSteady(), rec);

            long offset = rec.offset();
            Object key = rec.key();
            OpMessage op = rec.value();
            UUID opClientId = op.getClientId();
            byte opType = op.getOpType();

            FlushNotification flushNotification = null;
            boolean updated = false;

            if (key == null) {
                assert opType == OP_FLUSH_NOTIFICATION : opType;
                flushNotification = (FlushNotification) op;
                receivedFlushNotifications.increment();

                log.debug("Received flush notification for partition {}: {}", opsPart, rec);
            } else {
                var updateOp = (MapUpdate) op;
                receivedUpdates.increment();

                updated = updateHandler.applyReceivedUpdate(
                        rec.topic(),
                        rec.partition(),
                        offset,
                        opClientId,
                        updateOp.getOpId(),
                        opType,
                        key,
                        updateOp.getExpectedValue(),
                        updateOp.getUpdatedValue(),
                        updateOp.getFunction(),
                        updatedValue);

//                trace.trace("applyOpsTopicRecords updated={}, needFlush={}, key={}, val={}",
//                    updated, needFlush, key, updatedValueBox.get());
            }

            long oldMaxAddedOffset = flushQueue.addOpsRecord(key, updatedValue.value, offset, updated, flushNotification);

            if (needFlush(opClientId, oldMaxAddedOffset, offset))
                sendFlushRequest(opsPart, offset);
        }
    }

    protected boolean needFlush(UUID opClientId, long oldMaxAddedOffset, long recOffset) {
        if (clientId.equals(opClientId)) {
            assert recOffset > oldMaxAddedOffset;

            // Most of the time we will have just one or two iterations here because of skipped commit offsets,
            // we may have more iterations when kafka tx failed, and we skip multiple partially committed records,
            // but that would be an exceptional case.
            for (long offset = oldMaxAddedOffset + 1; offset <= recOffset; offset++) {
                if (offset % flushPeriodOps == 0)
                    return offset != 0;
            }
        }
        return false;
    }

    protected void sendFlushRequest(TopicPartition opsPart, long opsOffset) {
        ProducerRecord<Object,FlushRequest> rec = new ProducerRecord<>(flushTopic, opsPart.partition(),
            null, newFlushRequest(opsOffset));

        log.debug("Sending flush request for partition {}: {}", opsPart, rec);

        flushProducer.send(rec, (meta, err) -> {
            if (err == null)
                sentFlushRequests.increment();
        });
    }

    protected FlushRequest newFlushRequest(long opsOffset) {
        return new FlushRequest(clientId, opsOffset);
    }

    protected void seekOpsOffsets(Map<TopicPartition,Long> opsOffsets) {
        assert opsOffsets.size() == assignedOpsParts.size();
        opsConsumer.assign(opsOffsets.keySet());

        for (Map.Entry<TopicPartition,Long> entry : opsOffsets.entrySet()) {
            TopicPartition part = entry.getKey();
            long offset = entry.getValue();

            if (log.isDebugEnabled())
                log.debug("Seek ops consumer to {} for partition {}", offset, part);

//            trace.trace("seek offset={}, part={}", offset, part);

            opsConsumer.seek(part, offset);
            flushQueues.get(part.partition()).initUnflushedOpsOffset(offset - 1); // the last processed offset is expected here

            checkInterrupted();
        }
    }

    protected void processOps() {
        Duration pollTimeout = Duration.ofSeconds(1);

        while (!isInterrupted()) {
            ConsumerRecords<Object,OpMessage> recs;
            try {
                recs = Utils.poll(opsConsumer, pollTimeout);
            }
            catch (InterruptException | WakeupException e) {
                if (log.isDebugEnabled())
                    log.debug("Poll interrupted for partitions: {}", assignedOpsParts);

                return;
            }

            if (processOpsRecords(recs)) {
                if (log.isDebugEnabled())
                    log.debug("Steady for partitions: {}", assignedOpsParts);
            }
        }
    }

    protected boolean processOpsRecords(ConsumerRecords<Object,OpMessage> recs) {
        for (TopicPartition part : recs.partitions())
            applyOpsTopicRecords(part, recs.records(part));

        return !isSteady() && isActuallySteady() && markSteady();
    }

    protected boolean isSteady() {
        return steadyFut.isDone();
    }

    protected boolean markSteady() {
        return steadyFut.complete(null);
    }

    protected boolean isActuallySteady() {
        boolean freshEndOffsetsFetched = false;

        for (;;) {
            if (endOffsetsOps == null) {
                Set<TopicPartition> parts = opsConsumer.assignment();
                endOffsetsOps = opsConsumer.endOffsets(parts);
                freshEndOffsetsFetched = true;
            }

            long totalLag = 0;

            for (Map.Entry<TopicPartition,Long> entry : endOffsetsOps.entrySet()) {
                long endOffset = entry.getValue();
                TopicPartition part = entry.getKey();

                totalLag += endOffset - opsConsumer.position(part);
            }

            if (totalLag <= maxAllowedSteadyLag) {
                // we either need to refresh offsets and check the lag once again
                // or just cleanup before returning true
                endOffsetsOps = null;

                if (freshEndOffsetsFetched)
                    return true; // it was freshly fetched offsets, we are really steady

                // Initially maxAllowedSteadyLag is 0 to make sure
                // that we've achieved at least the first fetched endOffsetsOps.
                // This is needed in the case when we have only a single manager, in a single thread
                // we do a few updates, stop the manager and restart it. We expect to see all of
                // our updates to be in place when start operation completes but if
                // maxAllowedSteadyLag is non-zero we may have some updates lagged.
                // This behavior looks counter-intuitive from the user perspective
                // because it breaks program order. Thus, we must fetch all the
                // operations we aware of at the moment of start to safely complete steadyFut.
                maxAllowedSteadyLag = flushPeriodOps;
                continue;
            }
            return false;
        }
    }

    @Override
    protected void doRun() {
        try {
            Map<TopicPartition,Long> opsOffsets = loadData();
            seekOpsOffsets(opsOffsets);
            processOps();
        }
        catch (Exception e) {
            // If the future is completed already, nothing will happen.
            steadyFut.completeExceptionally(e);

            if (!Utils.isInterrupted(e))
                throw new ReplicaMapException(e);
        }
    }

    public CompletableFuture<Void> getSteadyFuture() {
        return steadyFut;
    }

    @Override
    protected void interruptThread() {
        Utils.wakeup(dataConsumer);
        Utils.wakeup(opsConsumer);

        super.interruptThread();
    }

    @Override
    public void close() {
        Utils.close(dataConsumer);
        Utils.close(opsConsumer);
    }

    protected static class UpdatedValue implements java.util.function.Consumer<Object> {
        protected Object value;

        @Override
        public void accept(Object o) {
            value = o;
        }

        public void clear() {
            value = null;
        }
    }
}
