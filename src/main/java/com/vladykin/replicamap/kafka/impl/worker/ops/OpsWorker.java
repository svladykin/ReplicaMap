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
import java.time.Duration;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_PUT;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_REMOVE_ANY;
import static com.vladykin.replicamap.kafka.impl.util.Utils.isOverMaxOffset;
import static com.vladykin.replicamap.kafka.impl.util.Utils.millis;
import static com.vladykin.replicamap.kafka.impl.util.Utils.trace;
import static java.util.Collections.singleton;

/**
 * Loads the initial state from the `data` topic and then
 * polls the `ops` topic and applies the updates to the inner map.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class OpsWorker extends Worker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(OpsWorker.class);

    protected static final ConsumerRecord<Object,FlushNotification> NOT_FOUND =
        new ConsumerRecord<>("NOT_FOUND", 0, 0, null, null);

    protected static final ConsumerRecord<Object,FlushNotification> NOT_EXIST =
        new ConsumerRecord<>("NOT_EXIST", 0, 0, null, null);

    protected final long clientId;

    protected final String dataTopic;
    protected final String opsTopic;
    protected final String flushTopic;

    protected final Set<Integer> assignedParts;

    protected final Consumer<Object,Object> dataConsumer;
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

        this.clientId = clientId;
        this.dataTopic = dataTopic;
        this.opsTopic = opsTopic;
        this.flushTopic = flushTopic;
        this.dataConsumer = dataConsumer;
        this.opsConsumer = opsConsumer;
        this.flushProducer = flushProducer;
        this.assignedParts = assignedParts;
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
            Duration pollTimeout = millis(1);
            Map<TopicPartition,Long> opsOffsets = new HashMap<>();

            for (TopicPartition dataPart : getAssignedTopicPartitions(dataTopic)) {
                log.debug("Loading data for partition {}", dataPart);

                TopicPartition opsPart = new TopicPartition(opsTopic, dataPart.partition());
                ConsumerRecord<Object,FlushNotification> lastFlushRec = findLastFlushNotification(dataPart, opsPart, pollTimeout);

                long flushOffsetOps = 0L;

                if (lastFlushRec != null) {
                    FlushNotification op = lastFlushRec.value();
                    flushOffsetOps = op.getFlushOffsetOps() + 1; // + 1 because we need the first unflushed ops offset
                    long flushOffsetData = op.getFlushOffsetData();

                    if (log.isDebugEnabled())
                        log.debug("Found last flush record {} for partition {}", lastFlushRec, opsPart);

                    loadDataForPartition(dataPart, flushOffsetData, pollTimeout);
                    lastFlushNotifications.put(opsPart, op);

//                    trace.trace("last {} : {}", opsPart, lastFlushRec);
                }
                else
                    log.debug("Flush record does not exist for partition {}", opsPart);

                opsOffsets.put(opsPart, flushOffsetOps);

                checkInterrupted();
            }

            return opsOffsets;
        }
        finally {
            Utils.close(dataConsumer); // We do not need it anymore.
        }
    }

    protected int loadDataForPartition(TopicPartition dataPart, long flushOffsetData, Duration pollTimeout) {
        dataConsumer.assign(singleton(dataPart));
        dataConsumer.seekToBeginning(singleton(dataPart));

        int loadedRecsCnt = 0;
        long lastRecOffset = -1;

        Map<Object,Object> loadedData = new HashMap<>(); // log.isTraceEnabled() ? new HashMap<>() : null;

        outer: for (;;) {
            ConsumerRecords<Object,Object> recs = dataConsumer.poll(pollTimeout);

            if (recs.isEmpty()) {
                long endOffsetData = Utils.endOffset(dataConsumer, dataPart);

                if (log.isDebugEnabled()) {
                    log.debug("Empty records while loading data for partition {}, endOffsetData: {}, " +
                                "flushOffsetData: {}, loadedRecs: {}, lastRecOffset: {}",
                                dataPart, endOffsetData, flushOffsetData, loadedRecsCnt, lastRecOffset);
                }

                if (endOffsetData <= flushOffsetData) { // flushOffsetData is inclusive, endOffsetData is exclusive
                    throw new ReplicaMapException("Too low end offset of the data partition: " +
                        dataPart + ", endOffsetData: " + endOffsetData + ", flushOffsetData: " + flushOffsetData);
                }

                if (dataConsumer.position(dataPart) == endOffsetData)
                    break; // we've loaded all the available data records
            }
            else {
                assert recs.partitions().size() == 1;

                for (ConsumerRecord<Object,Object> rec : recs.records(dataPart)) {
                    if (isOverMaxOffset(rec, flushOffsetData))
                        break outer;

                    loadedRecsCnt++;
                    lastRecOffset = rec.offset();

                    log.trace("Loading data partition {}, record: {}", dataPart, rec);

                    applyDataTopicRecord(rec);
                    collectDataRecord(loadedData, rec);

                    if (rec.offset() == flushOffsetData)
                        break outer; // it was the last record we need
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("Loaded {} data records for partition {}: {}", loadedRecsCnt, dataPart, loadedData);

//        for (Map.Entry<Object,Object> entry : loadedData.entrySet())
//            trace.trace("loadDataForPartition {} key={}, val={}", dataPart, entry.getKey(), entry.getValue());

        return loadedRecsCnt;
    }

    protected void collectDataRecord(Map<Object,Object> loadedData, ConsumerRecord<Object,Object> rec) {
        if (loadedData != null) {
            Object k = rec.key();
            Object v = rec.value();

            if (v == null)
                loadedData.remove(k);
            else
                loadedData.put(k, v);
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

            trace.trace("offset:{}, sentFlushReq: {}", err != null ? err.toString() : meta.offset(), rec);
        });
    }

    protected FlushRequest newFlushRequest(long flushOffsetOps, long lastCleanOffsetOps) {
        return new FlushRequest(clientId, flushOffsetOps, lastCleanOffsetOps);
    }

    protected ConsumerRecord<Object,FlushNotification> findLastFlushNotification(
        TopicPartition dataPart,
        TopicPartition opsPart,
        Duration pollTimeout
    ) {
        opsConsumer.assign(singleton(opsPart));
        long maxOffset = Utils.endOffset(opsConsumer, opsPart);

        for (;;) {
            ConsumerRecord<Object,FlushNotification> lastFlushRec =
                tryFindLastFlushNotification(opsPart, maxOffset, pollTimeout);

            if (lastFlushRec == NOT_EXIST)
                return null;

            if (lastFlushRec != NOT_FOUND && isValidFlushNotification(dataPart, lastFlushRec))
                return lastFlushRec;

            maxOffset -= flushPeriodOps;
        }
    }

    protected boolean isValidFlushNotification(
        TopicPartition dataPart,
        ConsumerRecord<Object,FlushNotification> lastFlushRec
    ) {
        // Sometimes Kafka provides smaller offset than actually is known to be committed.
        // In that case we have to find earlier flush record to be able to load the data.
        long endOffsetData = Utils.endOffset(dataConsumer, dataPart);
        if (endOffsetData > lastFlushRec.value().getFlushOffsetData())
            return true;

        log.warn("Committed offset is not found in data partition {}, end offset: {}, flush record: {}",
            dataPart, endOffsetData, lastFlushRec);

        return false;
    }

    protected ConsumerRecord<Object,FlushNotification> tryFindLastFlushNotification(
        TopicPartition opsPart,
        long maxOffset,
        Duration pollTimeout
    ) {
        long offset = maxOffset - flushPeriodOps;
        if (offset < 0)
            offset = 0;

        if (log.isDebugEnabled()) {
            log.debug("Searching for the last flush notification for partition {}, seek to {}, flushPeriodOps: {}",
                opsPart, offset, flushPeriodOps);
        }

        opsConsumer.seek(opsPart, offset);

        int processedRecs = 0;
        ConsumerRecord<Object,FlushNotification> lastFlush = null;

        outer: for (;;) {
            ConsumerRecords<Object,OpMessage> recs = opsConsumer.poll(pollTimeout);

            if (recs.isEmpty()) {
                if (Utils.isEndPosition(opsConsumer, opsPart))
                    break;
                else
                    continue;
            }

            for (ConsumerRecord<Object,OpMessage> rec : recs.records(opsPart)) {
                processedRecs++;

                if (log.isTraceEnabled())
                    log.trace("Searching through record: {}", rec);

                if (rec.value().getOpType() == OP_FLUSH_NOTIFICATION) {
                    lastFlush = Utils.cast(rec);
                    break outer;
                }

                if (rec.offset() > maxOffset)
                    break outer;
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Searched through {} records in partition {}, last flush notification record: {}",
                processedRecs, opsPart, lastFlush);
        }

        if (lastFlush != null)
            return lastFlush;

        return offset == 0 ? NOT_EXIST : NOT_FOUND;
    }

    protected void seekOpsOffsets(Map<TopicPartition,Long> opsOffsets) {
        assert opsOffsets.size() == assignedParts.size();
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
        Duration pollTimeout = Utils.millis(3);

        while (!isInterrupted()) {
            ConsumerRecords<Object,OpMessage> recs;
            try {
                recs = opsConsumer.poll(pollTimeout);
            }
            catch (InterruptException | WakeupException e) {
                if (log.isDebugEnabled())
                    log.debug("Poll interrupted for partitions: {}", getAssignedTopicPartitions(opsTopic));

                return;
            }

            if (processOpsRecords(recs)) {
                if (log.isDebugEnabled())
                    log.debug("Steady for partitions: {}", getAssignedTopicPartitions(opsTopic));

                pollTimeout = Utils.seconds(3);
            }
        }
    }

    protected List<TopicPartition> getAssignedTopicPartitions(String topic) {
        return assignedParts.stream()
            .map(part -> new TopicPartition(topic, part))
            .collect(Collectors.toList());
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
