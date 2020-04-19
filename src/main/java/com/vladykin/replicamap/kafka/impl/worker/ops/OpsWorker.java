package com.vladykin.replicamap.kafka.impl.worker.ops;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.msg.FlushNotification;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import com.vladykin.replicamap.kafka.impl.msg.MapUpdate;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.util.Box;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import com.vladykin.replicamap.kafka.impl.worker.Worker;
import com.vladykin.replicamap.kafka.impl.worker.flush.FlushQueue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
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

import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_PUT;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_REMOVE_ANY;
import static com.vladykin.replicamap.kafka.impl.util.Utils.MIN_POLL_TIMEOUT_MS;
import static com.vladykin.replicamap.kafka.impl.worker.flush.FlushWorker.OPS_OFFSET_HEADER;
import static java.util.Collections.singleton;

/**
 * Loads the initial state from the `data` topic and then
 * polls the `ops` topic and applies the updates to the inner map.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class OpsWorker extends Worker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(OpsWorker.class);

    protected final long clientId;

    protected final String dataTopic;
    protected final String opsTopic;
    protected final String flushTopic;

    protected final List<TopicPartition> assignedOpsParts;

    protected Consumer<Object,Object> dataConsumer;
    protected final Consumer<Object,OpMessage> opsConsumer;
    protected final Producer<Object,FlushRequest> flushProducer;

    protected final int flushPeriodOps;
    protected final List<FlushQueue> flushQueues;
    protected final Queue<ConsumerRecord<Object,FlushNotification>> cleanQueue;

    protected final OpsUpdateHandler updateHandler;

    protected final CompletableFuture<Void> steadyFut = new CompletableFuture<>();
    protected Map<TopicPartition,Long> endOffsetsOps;
    protected int maxAllowedSteadyLag;

    protected final Map<TopicPartition,FlushNotification> lastFlushNotifications = new HashMap<>();

    protected final LongAdder sentFlushRequests;
    protected final LongAdder receivedUpdates;
    protected final LongAdder receivedDataRecords;
    protected final LongAdder receivedFlushNotifications;

    public OpsWorker(
        long clientId,
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
        Queue<ConsumerRecord<Object,FlushNotification>> cleanQueue,
        OpsUpdateHandler updateHandler,
        LongAdder sentFlushRequests,
        LongAdder receivedUpdates,
        LongAdder receivedDataRecords,
        LongAdder receivedFlushNotifications
    ) {
        super("replicamap-ops-" + dataTopic + "-" +
            Long.toHexString(clientId), workerId);

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
        this.cleanQueue = cleanQueue;
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
            ConsumerRecords<Object,Object> recs = Utils.poll(dataConsumer, MIN_POLL_TIMEOUT_MS);

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

        receivedDataRecords.increment();
        updateHandler.applyReceivedUpdate(dataRec.topic(), dataRec.partition(), dataRec.offset(),
            0L, 0L, opType, key, null, val, null, null);
    }

    protected void applyOpsTopicRecords(TopicPartition opsPart, List<ConsumerRecord<Object,OpMessage>> partRecs) {
        FlushQueue flushQueue = flushQueues.get(opsPart.partition());

        int lastIndex = partRecs.size() - 1;
        Box<Object> updatedValueBox = new Box<>();

        for (int i = 0; i <= lastIndex; i++) {
            updatedValueBox.clear();
            ConsumerRecord<Object,OpMessage> rec = partRecs.get(i);

            if (log.isTraceEnabled())
                log.trace("Applying op to partition {}, steady: {}, record: {}", opsPart, isSteady(), rec);

            Object key = rec.key();
            OpMessage op = rec.value();
            long opClientId = op.getClientId();
            byte opType = op.getOpType();

            boolean updated = false;
            boolean needClean = false;
            boolean needFlush = opClientId == clientId && rec.offset() > 0 && rec.offset() % flushPeriodOps == 0;

            if (key == null) {
                if (opType == OP_FLUSH_NOTIFICATION) {
                    FlushNotification flush = (FlushNotification)op;
                    receivedFlushNotifications.increment();

                    FlushNotification old = lastFlushNotifications.get(opsPart);
                    // Notifications can arrive out of order, just ignore the outdated ones.
                    if (old == null || old.getFlushOffsetOps() < flush.getFlushOffsetOps()) {
                        // If someone else has successfully flushed the data, we need to cleanup our flush queue.
                        needClean = opClientId != clientId;
                        lastFlushNotifications.put(opsPart, flush);
                        log.debug("Received flush notification for partition {}: {}", opsPart, rec);
                    }
                }
                else // Forward compatibility: there are may be new message types.
                    log.warn("Unexpected op type: {}", (char)op.getOpType());
            }
            else {
                MapUpdate updateOp = (MapUpdate)op;
                receivedUpdates.increment();

                updated = updateHandler.applyReceivedUpdate(
                    rec.topic(),
                    rec.partition(),
                    rec.offset(),
                    opClientId,
                    updateOp.getOpId(),
                    opType,
                    key,
                    updateOp.getExpectedValue(),
                    updateOp.getUpdatedValue(),
                    updateOp.getFunction(),
                    updatedValueBox);

//                trace.trace("applyOpsTopicRecords updated={}, needFlush={}, key={}, val={}",
//                    updated, needFlush, key, updatedValueBox.get());
            }

            flushQueue.add(
                updated ? key : null,
                updatedValueBox.get(),
                rec.offset(),
                needClean || needFlush || i == lastIndex);

            if (needFlush) {
                FlushNotification lastFlush = lastFlushNotifications.get(opsPart);
                long lastCleanOffsetOps = lastFlush == null ? -1L : lastFlush.getFlushOffsetOps();
                sendFlushRequest(opsPart, rec.offset(), lastCleanOffsetOps);
            }
            else if (needClean)
                sendCleanRequest(opsPart, Utils.cast(rec));
        }
    }

    protected void sendCleanRequest(TopicPartition opsPart, ConsumerRecord<Object,FlushNotification> rec) {
        log.debug("Sending clean request for partition {}: {}", opsPart, rec);
        cleanQueue.add(rec);
    }

    protected void sendFlushRequest(TopicPartition opsPart, long flushOffsetOps, long lastCleanOffsetOps) {
        ProducerRecord<Object,FlushRequest> rec = new ProducerRecord<>(flushTopic, opsPart.partition(),
            null, newFlushRequest(flushOffsetOps, lastCleanOffsetOps));

        log.debug("Sending flush request for partition {}: {}", opsPart, rec);

        flushProducer.send(rec, (meta, err) -> {
            if (err == null)
                sentFlushRequests.increment();
        });
    }

    protected FlushRequest newFlushRequest(long flushOffsetOps, long lastCleanOffsetOps) {
        return new FlushRequest(clientId, flushOffsetOps, lastCleanOffsetOps);
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
            flushQueues.get(part.partition()).setMaxOffset(offset - 1); // the last processed offset is expected here

            checkInterrupted();
        }
    }

    protected void processOps() {
        while (!isInterrupted()) {
            ConsumerRecords<Object,OpMessage> recs;
            try {
                recs = Utils.poll(opsConsumer, 1000);
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
}
