package com.vladykin.replicamap.kafka.impl.worker.ops;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.KReplicaMapManager;
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
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
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

    /** Placeholder client id for data-topic records (they have no originating client). */
    protected static final UUID ZERO_UUID = new UUID(0, 0);

    protected final UUID clientId;

    protected final String dataTopic;
    protected final String opsTopic;
    protected final String flushTopic;

    protected final List<TopicPartition> assignedOpsParts;

    protected volatile Consumer<Object,Object> dataConsumer;
    protected final Consumer<Object,OpMessage> opsConsumer;
    protected final Producer<Object,FlushRequest> flushProducer;

    /** How often the idle ops poll wakes to run the time-based flush check (a soft ~50ms granularity
     * on flush.max.unflushed.ms; active partitions are unaffected, poll returns at once on data). */
    protected static final Duration POLL_TIMEOUT = Duration.ofMillis(50);

    /** Sentinel for a data record that carries no {@code replicamap.ops} header (e.g. an adopted topic). */
    protected static final long NO_HEADER = Long.MIN_VALUE;

    /** Sentinel for a disabled time-based flush trigger, see {@link #maybeRequestIdleFlush()}. */
    protected static final long IDLE_FLUSH_DISABLED = -1;

    protected final int flushPeriodOps;

    /** Size of the first backward window of the resume notification scan; each next window doubles. */
    protected final long resumeScanInitWindowOps;

    /** Retries of the resume resolution when flushes keep landing (and their ops keep getting
     * retention-deleted) concurrently with the load; each retry requires the data snapshot to have
     * advanced, so the cap only trips on pathological churn. */
    protected static final int MAX_RESUME_RESOLUTION_ATTEMPTS = 10;

    protected final long flushMaxUnflushedNanos;
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
        Duration flushMaxUnflushed,
        List<FlushQueue> flushQueues,
        KReplicaMapManager manager,
        OpsUpdateHandler updateHandler,
        LongAdder sentFlushRequests,
        LongAdder receivedUpdates,
        LongAdder receivedDataRecords,
        LongAdder receivedFlushNotifications
    ) {
        super("replicamap-ops-" + dataTopic + "-" + workerId + "-" + clientId, manager);

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
        this.resumeScanInitWindowOps = 2L * flushPeriodOps;
        // Negative means the time-based flush trigger is disabled, see maybeRequestIdleFlush.
        this.flushMaxUnflushedNanos = flushMaxUnflushed.isNegative() ?
            IDLE_FLUSH_DISABLED : Utils.toNanosSaturated(flushMaxUnflushed);
        this.flushQueues = flushQueues;
        this.updateHandler = updateHandler;
        this.sentFlushRequests = sentFlushRequests;
        this.receivedUpdates = receivedUpdates;
        this.receivedDataRecords = receivedDataRecords;
        this.receivedFlushNotifications = receivedFlushNotifications;
    }

    protected Map<TopicPartition, OpsResume> loadData() {
        try {
            Map<TopicPartition,OpsResume> opsResumes = new HashMap<>();

            for (TopicPartition opsPart : assignedOpsParts) {
                TopicPartition dataPart = new TopicPartition(dataTopic, opsPart.partition());
                log.debug("Loading data for partition {}", dataPart);

                opsResumes.put(opsPart, loadDataForPartition(dataPart, opsPart));
                checkInterrupted();
            }

            return opsResumes;
        }
        finally {
            Utils.close(dataConsumer); // We do not need it anymore.
            dataConsumer = null;
        }
    }

    /**
     * Loads the compacted data-topic snapshot for the partition and resolves the exact ops resume
     * point — no consumer-group {@code committed()} call, so startup does not depend on the flush
     * coordinator. Two complementary sources are combined:
     * <ul>
     * <li>The max {@code replicamap.ops} header among the loaded records (the {@code candidate}):
     *     survives as long as the data does, but may lag the latest reflected flush when delete-only
     *     flushes were compacted away (tombstones take their headers with them).</li>
     * <li>The flush notifications in the ops topic: exact (each carries the offset of the last data
     *     record its flush wrote), but they age out with the ops retention. The latest notification
     *     whose flushed data lies inside our snapshot marks the latest flush the snapshot reflects —
     *     the exact resume point (see {@link #findResumeByOpsScan}).</li>
     * </ul>
     * If the range right after the resume point was retention-deleted, the deletion is verified
     * against the data topic before being skipped: ops may be deleted only once flushed, so either
     * a re-read of the data tail picks up the flush that made them deletable (and the resolution is
     * retried with the advanced snapshot), or the snapshot is stable, which proves the deleted range
     * contains nothing the snapshot does not already reflect.
     */
    protected OpsResume loadDataForPartition(TopicPartition dataPart, TopicPartition opsPart) {
        dataConsumer.assign(singleton(dataPart));
        dataConsumer.seekToBeginning(singleton(dataPart));

        DataLoadState data = new DataLoadState();
        readDataToEnd(dataPart, data);

        for (int attempt = 1;; attempt++) {
            long opsHead = Utils.endOffset(opsConsumer, opsPart); // read_committed LSO via ListOffsets.

            FoundNotification found = findResumeByOpsScan(opsPart, opsHead, data.dataEnd, data.candidate);

            long resume = found != null ? found.opsOffset : data.candidate;
            long seekOffset = resume + 1; // The first ops position to replay from.

            long opsLogStart = Utils.beginningOffset(opsConsumer, opsPart);

            if (seekOffset >= opsLogStart)
                return new OpsResume(seekOffset, resume);

            // The ops range right after the resume point was retention-deleted. Only flushed ops may
            // be deleted, so their data must be in the data topic: re-read the tail to verify.
            long dataEndBefore = data.dataEnd;
            readDataToEnd(dataPart, data);

            if (data.dataEnd > dataEndBefore) {
                // A flush landed after our snapshot (that is what made those ops deletable): the tail
                // we just read reflects it, so retry the resolution with the advanced snapshot.
                if (attempt >= MAX_RESUME_RESOLUTION_ATTEMPTS) {
                    throw new ReplicaMapException("Failed to resolve the ops resume point for partition " +
                        opsPart + " after " + attempt + " attempts: flushes keep racing the load.");
                }

                checkInterrupted();
                continue;
            }

            // Stable snapshot: no flush landed after it, so the deleted range can only contain updates
            // the snapshot already reflects, plus notifications and failed-op noise. Skip it.
            log.warn("Ops records of {} were deleted in range [{}, {}), resuming from the log start. " +
                "This is expected for a partition that was idle past the ops topic retention; otherwise " +
                "make sure the retention covers the unflushed ops plus the client restart time.",
                opsPart, seekOffset, opsLogStart);

            // The resume assertion stays armed only if the resume notification was actually found at or
            // above the skip point (then it will be replayed); with the candidate fallback it may sit
            // inside the skipped range.
            boolean armed = found != null && found.position >= opsLogStart;

            return new OpsResume(opsLogStart, armed ? resume : -1);
        }
    }

    /**
     * Reads the data partition from the current position to the end, applying every record to the map.
     * On the first call the caller seeks to the beginning; subsequent calls read only the newly
     * appended tail (appends are tail-only and compaction never reorders, and data records are
     * absolute states, so applying the tail over the already loaded snapshot converges).
     * <p>
     * Accumulates into {@code state}: the max {@code replicamap.ops} header seen ({@code candidate},
     * {@code -1} if none) and the end offset the partition was read to ({@code dataEnd}). Headerless
     * records (a pre-existing / adopted data topic) are allowed only as a prefix — once a headered
     * record has been seen, a later headerless record is a data-integrity violation and fails the load.
     */
    protected void readDataToEnd(TopicPartition dataPart, DataLoadState state) {
        long endOffset = Utils.endOffset(dataConsumer, dataPart); // With read_committed endOffset means LSO.

        for (;;) {
            ConsumerRecords<Object,Object> recs = Utils.poll(dataConsumer, MIN_POLL_TIMEOUT);

            if (recs.isEmpty()) {
                // Re-fetch the end offset only once we have reached the last fetched one, to avoid an
                // endOffset RPC on every empty poll (empty polls are common: fetch in flight, or
                // read_committed filtering out control/aborted records while position still advances).
                long offset = dataConsumer.position(dataPart);
                if (offset >= endOffset) {
                    endOffset = Utils.endOffset(dataConsumer, dataPart);

                    // read_committed: position can never be past the LSO, and the LSO only grows.
                    assert offset <= endOffset : offset + " > " + endOffset;

                    if (offset >= endOffset) {
                        state.dataEnd = offset;
                        return;
                    }
                }

                continue;
            }

            for (ConsumerRecord<Object,Object> rec : recs.records(dataPart)) {
                log.trace("Loading data partition {}, record: {}", dataPart, rec);
                applyDataTopicRecord(rec);

                long ops = readOpsOffsetHeader(rec);

                if (ops == NO_HEADER) {
                    if (state.sawHeader) {
                        throw new ReplicaMapException("Data record without '" + OPS_OFFSET_HEADER +
                            "' header after a headered record (external writes or corruption): " + rec);
                    }
                }
                else {
                    state.sawHeader = true;
                    if (ops > state.candidate)
                        state.candidate = ops;
                }
            }

            checkInterrupted();
        }
    }

    /**
     * Scans the ops topic backward from the head, in doubling windows, for the latest flush
     * notification whose flushed data lies inside our snapshot ({@code dataOffset < dataEnd}).
     * Notifications are strictly monotonic in both offsets (one exclusive, fenced flusher per
     * partition), so along the log they belong to our snapshot up to exactly one point and never
     * after it. Hence each slice is unambiguous: if it contains a qualifying notification, the
     * latest one is exactly the resume notification (everything above it was already scanned and
     * was too new); if not, the resume notification lies strictly below — double and continue.
     * <p>
     * The scan never goes below {@code candidate + 1} (a notification sits after the ops it flushed)
     * or below the ops log start. Retention may legitimately delete flushed ops we are scanning
     * across, even concurrently: an out-of-range poll is handled by continuing from the live log
     * start, which only grows, so the scan always converges.
     *
     * @return The found resume notification, or {@code null} if none is retained (the caller falls
     *         back to the candidate).
     */
    protected FoundNotification findResumeByOpsScan(TopicPartition opsPart, long opsHead, long dataEnd, long candidate) {
        long floor = candidate + 1;

        if (opsHead <= floor)
            return null; // Nothing to scan: no ops after the candidate.

        opsConsumer.assign(singleton(opsPart));

        long sliceEnd = opsHead;
        boolean lastSlice = false;

        for (long back = resumeScanInitWindowOps; !lastSlice; back *= 2) {
            long sliceStart = back > 0 ? opsHead - back : floor; // back overflows on absurd ranges.

            if (sliceStart <= floor) {
                sliceStart = floor;
                lastSlice = true;
            }

            log.debug("Scanning ops slice [{}, {}) of {} for the resume flush notification",
                sliceStart, sliceEnd, opsPart);

            FoundNotification found = null;
            opsConsumer.seek(opsPart, sliceStart);

            while (opsConsumer.position(opsPart) < sliceEnd) {
                ConsumerRecords<Object,OpMessage> recs;
                try {
                    recs = Utils.poll(opsConsumer, MIN_POLL_TIMEOUT);
                }
                catch (OffsetOutOfRangeException e) {
                    // Retention deleted a range we are scanning across (legitimate for flushed ops, even
                    // concurrently with this scan). Continue from the live log start: it only grows, so
                    // this converges. Nothing exists below it, thus this slice is the last one.
                    long logStart = Utils.beginningOffset(opsConsumer, opsPart);
                    log.debug("Ops scan of {} hit a deleted range in slice [{}, {}), continuing from the log start {}",
                        opsPart, sliceStart, sliceEnd, logStart);

                    lastSlice = true;

                    if (logStart >= sliceEnd)
                        break; // The whole remaining slice is gone.

                    opsConsumer.seek(opsPart, logStart);
                    checkInterrupted();
                    continue;
                }

                for (ConsumerRecord<Object,OpMessage> rec : recs.records(opsPart)) {
                    if (rec.offset() >= sliceEnd)
                        break; // A fetch may overrun the slice into the already scanned range above.

                    OpMessage op = rec.value();

                    if (op.getOpType() == OP_FLUSH_NOTIFICATION) {
                        FlushNotification fn = (FlushNotification)op;

                        if (fn.getDataOffset() < dataEnd)
                            found = new FoundNotification(fn.getOpsOffset(), rec.offset());
                    }
                }

                checkInterrupted();
            }

            if (found != null) {
                log.debug("Resolved the resume point for {} by ops scan: {}", opsPart, found);
                return found;
            }

            sliceEnd = sliceStart;
        }

        return null; // No qualifying notification is retained.
    }

    /** Accumulated state of the data-topic snapshot load, see {@link #readDataToEnd}. */
    protected static class DataLoadState {
        /** Max {@code replicamap.ops} header among the loaded records, -1 if none. */
        long candidate = -1;

        /** Whether any loaded record carried the header (integrity check for adopted topics). */
        boolean sawHeader;

        /** The offset the data partition was read up to (read_committed LSO at load end). */
        long dataEnd = -1;
    }

    /** A flush notification located by {@link #findResumeByOpsScan}. */
    protected static class FoundNotification {
        /** The flushed ops offset the notification carries (the resume point). */
        final long opsOffset;

        /** The position of the notification record itself in the ops topic. */
        final long position;

        FoundNotification(long opsOffset, long position) {
            this.opsOffset = opsOffset;
            this.position = position;
        }

        @Override
        public String toString() {
            return "FoundNotification{opsOffset=" + opsOffset + ", position=" + position + '}';
        }
    }

    /** Resolved ops resume point for a partition, see {@link #loadDataForPartition}. */
    protected static class OpsResume {
        /** The ops offset to seek to and replay from. */
        final long seekOffset;

        /** The flushed ops offset to arm the resume-consistency assertion with, -1 to disarm it
         * (never flushed, or the resume notification lies in a verified-skipped deleted range). */
        final long resumeOpsOffset;

        OpsResume(long seekOffset, long resumeOpsOffset) {
            this.seekOffset = seekOffset;
            this.resumeOpsOffset = resumeOpsOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OpsResume)) return false;

            OpsResume other = (OpsResume)o;
            return seekOffset == other.seekOffset && resumeOpsOffset == other.resumeOpsOffset;
        }

        @Override
        public int hashCode() {
            return 31 * Long.hashCode(seekOffset) + Long.hashCode(resumeOpsOffset);
        }

        @Override
        public String toString() {
            return "OpsResume{seekOffset=" + seekOffset + ", resumeOpsOffset=" + resumeOpsOffset + '}';
        }
    }

    /**
     * @return The {@code replicamap.ops} header (flushed ops offset) of the data record, or
     *         {@link #NO_HEADER} if the record carries no such header.
     */
    protected long readOpsOffsetHeader(ConsumerRecord<Object,Object> dataRec) {
        Header header = dataRec.headers().lastHeader(OPS_OFFSET_HEADER);
        return header == null ? NO_HEADER : Utils.deserializeVarlong(header.value());
    }

    protected void applyDataTopicRecord(ConsumerRecord<Object,Object> dataRec) {
        Object key = dataRec.key();
        Object val = dataRec.value();
        byte opType = val == null ? OP_REMOVE_ANY : OP_PUT;

        receivedDataRecords.increment();
        updateHandler.applyReceivedUpdate(dataRec.topic(), dataRec.partition(), dataRec.offset(),
            ZERO_UUID, 0L, opType, key, null, val, null, null);
    }

    protected void applyOpsTopicRecords(TopicPartition opsPart, List<ConsumerRecord<Object,OpMessage>> partRecs) {
        var flushQueue = flushQueues.get(opsPart.partition());
        var updatedValue = new UpdatedValue();

        // One timestamp for the whole batch: the idle-flush staleness threshold is a soft ~500ms, so
        // per-record precision is unneeded and this avoids a nanoTime() call per update op.
        long currentTimeNanos = System.nanoTime();

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

            long oldMaxAddedOffset = flushQueue.addOpsRecord(key, updatedValue.value, offset, updated, flushNotification, currentTimeNanos);

            if (needFlush(opClientId, oldMaxAddedOffset, offset))
                sendFlushRequest(opsPart, offset);
        }
    }

    /**
     * Defense-in-depth check that every assigned partition replayed the flush notification for its
     * resume offset by the time we reach the steady state (each {@link FlushQueue} verifies its own;
     * see {@link FlushQueue#checkFoundResumeFlushNotification()}).
     */
    protected void checkFoundAllResumeFlushNotifications() {
        for (TopicPartition opsPart : assignedOpsParts)
            flushQueues.get(opsPart.partition()).checkFoundResumeFlushNotification();
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
        sendFlushRequest(opsPart, opsOffset, null);
    }

    protected void sendFlushRequest(TopicPartition opsPart, long opsOffset, Runnable onSendError) {
        ProducerRecord<Object,FlushRequest> rec = new ProducerRecord<>(flushTopic, opsPart.partition(),
            null, newFlushRequest(opsOffset));

        log.debug("Sending flush request for partition {}: {}", opsPart, rec);

        flushProducer.send(rec, (meta, err) -> {
            if (err == null)
                sentFlushRequests.increment();
            else {
                log.warn("Failed to send flush request for partition {}: {}", opsPart, Utils.getMessage(err));

                if (onSendError != null)
                    onSendError.run();
            }
        });
    }

    protected FlushRequest newFlushRequest(long opsOffset) {
        return new FlushRequest(clientId, opsOffset);
    }

    protected void seekOpsOffsets(Map<TopicPartition,OpsResume> opsResumes) {
        assert opsResumes.size() == assignedOpsParts.size();
        opsConsumer.assign(opsResumes.keySet());

        for (Map.Entry<TopicPartition,OpsResume> entry : opsResumes.entrySet()) {
            TopicPartition part = entry.getKey();
            OpsResume resume = entry.getValue();

            if (log.isDebugEnabled())
                log.debug("Seek ops consumer to {} for partition {}", resume, part);

            opsConsumer.seek(part, resume.seekOffset);

            // The last processed offset is expected here; the resume assertion is armed separately
            // (-1 when the resume notification is not expected to be replayed).
            flushQueues.get(part.partition()).initUnflushedOpsOffset(resume.seekOffset - 1, resume.resumeOpsOffset);

            checkInterrupted();
        }
    }

    protected void processOps() {
        while (!isInterrupted()) {
            ConsumerRecords<Object,OpMessage> recs;
            try {
                recs = Utils.poll(opsConsumer, POLL_TIMEOUT);
            }
            catch (InterruptException | WakeupException e) {
                if (log.isDebugEnabled())
                    log.debug("Poll interrupted for partitions: {}", assignedOpsParts);

                return;
            }
            catch (OffsetOutOfRangeException e) {
                // Retention deleted ops at/after our replay position while we were catching up. Unlike
                // the load phase (which resolves this against the data topic), there is no safe recovery
                // here — the ops we still need are gone, so fail loudly (this propagates to
                // doRun -> onUnrecoverableError).
                //
                // The load resolves the resume point against a beginning offset read before it reads the
                // remaining partitions, so an idle partition whose already flushed tail is deleted in
                // that window lands here too, harmlessly. That needs a segment roll plus a retention
                // sweep inside the load, and a restart resolves it, hence just the hint below.
                throw new ReplicaMapException("Ops topic retention is too short: records were deleted at " +
                    "the replay position for partitions " + e.partitions() + ". Increase the ops topic " +
                    "retention to cover the unflushed ops plus the client restart time. If instead the " +
                    "partition was idle past the retention, restarting the client is enough.", e);
            }

            if (processOpsRecords(recs)) {
                if (log.isDebugEnabled())
                    log.debug("Steady for partitions: {}", assignedOpsParts);
            }

            maybeRequestIdleFlush();
        }
    }

    /**
     * Time-based flush trigger. An idle partition never has an update cross a {@code flush.period.ops}
     * boundary, so its unflushed tail would never reach the data topic. The flush-partition owner — and
     * only it, to avoid a herd of identical requests — requests a flush once the oldest unflushed update
     * is older than {@code flush.max.unflushed.ms}, reusing the normal flush-request path. Ownership is
     * read straight off the {@link FlushQueue} (set by the flush worker on assignment); in unit tests it
     * is never owned, so this is a no-op there.
     * <p>
     * Disabled entirely when {@code flush.max.unflushed.ms} is {@code -1}.
     */
    protected void maybeRequestIdleFlush() {
        if (flushMaxUnflushedNanos == IDLE_FLUSH_DISABLED)
            return;

        long currentTimeNanos = System.nanoTime();

        for (TopicPartition opsPart : assignedOpsParts) {
            int part = opsPart.partition();

            FlushQueue flushQueue = flushQueues.get(part);

            if (!flushQueue.isFlushOwned())
                continue;

            long offset = flushQueue.staleFlushOffset(currentTimeNanos, flushMaxUnflushedNanos);

            if (offset >= 0) {
                log.debug("Requesting time-based flush for partition {} up to offset {}", opsPart, offset);
                // Re-arm the trigger if the send fails, otherwise the tail stays unflushed
                // until the next update arrives (possibly never on an idle partition).
                sendFlushRequest(opsPart, offset, () -> flushQueue.onStaleFlushRequestFailed(offset));
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
        if (steadyFut.isDone())
            return false;

        // Verify before completing: a violation must fail startup (doRun completes
        // steadyFut exceptionally), not surface asynchronously after start() succeeded.
        checkFoundAllResumeFlushNotifications();

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
            Map<TopicPartition,OpsResume> opsResumes = loadData();
            seekOpsOffsets(opsResumes);
            processOps();
        }
        catch (Throwable e) {
            // If the future is completed already, nothing will happen.
            steadyFut.completeExceptionally(e);

            // Worker.run records the failure on the manager, so everything fails loudly
            // instead of this partition silently going stale.
            if (!Utils.isInterrupted(e))
                throw new ReplicaMapException(e);
        }
        finally {
            // processOps returns normally when the poll is interrupted, thus without this a worker
            // stopped before it reached the steady state would leave start() hanging forever.
            // No-op if the future is completed already. The InterruptedException cause makes
            // Utils.isInterrupted see this as an interruption, so that the flush workers waiting
            // for the steady future exit quietly instead of reporting an unrecoverable error.
            steadyFut.completeExceptionally(new ReplicaMapException(
                "Ops worker stopped before reaching the steady state.", new InterruptedException()));
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
