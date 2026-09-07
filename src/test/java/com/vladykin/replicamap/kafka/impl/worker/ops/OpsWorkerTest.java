package com.vladykin.replicamap.kafka.impl.worker.ops;

import com.vladykin.replicamap.kafka.impl.msg.FlushNotification;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import com.vladykin.replicamap.kafka.impl.msg.MapUpdate;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.serde.OpMessageSerializer;
import com.vladykin.replicamap.kafka.impl.worker.flush.FlushQueue;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.NO_LIMIT;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_REQUEST;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_PUT;
import static com.vladykin.replicamap.kafka.impl.worker.flush.FlushWorker.OPS_OFFSET_HEADER;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"unchecked", "FieldCanBeLocal", "UnusedReturnValue"})
public class OpsWorkerTest {
    public static final UUID CLIENT1_ID = UUID.randomUUID();
    public static final UUID CLIENT2_ID = UUID.randomUUID();

    public static final String TOPIC_DATA = "data";
    public static final String TOPIC_OPS = "ops";
    public static final String TOPIC_FLUSH = "flush";

    static int FLUSH_MAX_OPS = 10;

    Set<Integer> parts;
    RealisticMockConsumer<Object,Object> dataConsumer;
    RealisticMockConsumer<Object,OpMessage> opsConsumer;
    MockProducer<Object,FlushRequest> flushProducer;
    List<FlushQueue> flushQueues;
    OpsWorker opsWorker;
    TopicPartition dataPart;
    TopicPartition opsPart;
    TopicPartition flushPart;
    List<Object[]> appliedRecords; // {topic, offset, key, value} per applied update, in application order.
    Map<String,Long> lastAppliedOffsets;

    @SuppressWarnings("rawtypes")
    @BeforeEach
    void beforeEachTest() {
        appliedRecords = new ArrayList<>();
        lastAppliedOffsets = new HashMap<>();

        parts = singleton(0);
        opsPart = new TopicPartition(TOPIC_OPS, 0);
        dataPart = new TopicPartition(TOPIC_DATA, 0);
        flushPart = new TopicPartition(TOPIC_FLUSH, 0);

        flushQueues = singletonList(new FlushQueue(null));
        dataConsumer = new RealisticMockConsumer<>();
        opsConsumer = new RealisticMockConsumer<>();

        Serializer<?> longSer = new LongSerializer();
        Partitioner partitioner = null;

        flushProducer = new MockProducer(true, partitioner, longSer,
            new OpMessageSerializer<>(longSer, null));

        opsConsumer.assign(singleton(opsPart));
        dataConsumer.assign(singleton(dataPart));

        opsWorker = new OpsWorker(
            CLIENT1_ID, TOPIC_DATA, TOPIC_OPS, TOPIC_FLUSH,
            0,
            parts,
            dataConsumer,
            opsConsumer,
            flushProducer,
            FLUSH_MAX_OPS,
            Duration.ofSeconds(1),
            flushQueues,
            null,
            this::applyReceivedUpdate,
            new LongAdder(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder()
        );
    }

    protected <K, V> boolean applyReceivedUpdate(String topic, int part, long offset,
        UUID clientId, long opId, byte updateType, K key, V exp, V upd,
        BiFunction<?,?,?> function, Consumer<V> updatedValueBox) {
        // Free global invariant: loads, re-reads and replays must never go backward within a topic.
        Long last = lastAppliedOffsets.put(topic, offset);
        assertTrue(last == null || offset > last,
            "Non-ascending applied offset " + offset + " after " + last + " in topic " + topic);

        appliedRecords.add(new Object[] {topic, offset, key, upd});
        return true;
    }

    int appliedCount() {
        return appliedRecords.size();
    }

    List<Object> appliedKeys() {
        List<Object> keys = new ArrayList<>(appliedRecords.size());

        for (Object[] rec : appliedRecords)
            keys.add(rec[2]);

        return keys;
    }

    static ConsumerRecord<Object,OpMessage> newPutRecord(UUID clientId, long offset) {
        Random rnd = ThreadLocalRandom.current();
        return new ConsumerRecord<>(TOPIC_OPS, 0,
            offset, rnd.nextInt(10), new MapUpdate(OP_PUT, clientId, 0, null, rnd.nextInt(10), null));
    }

    public static ConsumerRecord<Object,OpMessage> newFlushNotification(UUID clientId, long flushOffsetOps, long dataOffset, long offset) {
        return new ConsumerRecord<>(TOPIC_OPS, 0, offset, null, new FlushNotification(clientId, flushOffsetOps, dataOffset));
    }

    /** Data-topic record carrying the {@code replicamap.ops} header (or no header when {@code opsHeader} is null). */
    static ConsumerRecord<Object,Object> newDataRecord(long offset, Object key, Object val, Long opsHeader) {
        RecordHeaders headers = new RecordHeaders();

        if (opsHeader != null)
            headers.add(new RecordHeader(OPS_OFFSET_HEADER, Utils.serializeVarlong(opsHeader)));

        return new ConsumerRecord<>(TOPIC_DATA, 0, offset, ConsumerRecord.NO_TIMESTAMP,
            TimestampType.NO_TIMESTAMP_TYPE, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE,
            key, val, headers, Optional.empty());
    }

    @Test
    void testSeekOpsOffsets() {
        opsWorker.seekOpsOffsets(singletonMap(opsPart, new OpsWorker.OpsResume(111511L, 111510L)));
        assertEquals(111511L, opsConsumer.position(opsPart));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void testApplyOpsTopicRecords() {
        // Resume from flushed ops offset 999: the first replayed record is its flush notification.
        opsWorker.flushQueues.get(opsPart.partition()).initUnflushedOpsOffset(999);

        opsWorker.applyOpsTopicRecords(opsPart, Arrays.asList(
                // Resume flush notification (CLIENT2 so it does not trip needFlush at offset 1000).
                newFlushNotification(CLIENT2_ID, 999, 0, 1000),
                newPutRecord(CLIENT1_ID, 1001),
                newPutRecord(CLIENT2_ID, 1002),
                newPutRecord(CLIENT1_ID, 1003),
                newFlushNotification(CLIENT1_ID, 1003, 0, 1004),
                newPutRecord(CLIENT1_ID, 1005),
                newPutRecord(CLIENT2_ID, 1006),
                newPutRecord(CLIENT1_ID, 1007),
                newFlushNotification(CLIENT2_ID, 1007, 0, 1008),
                newPutRecord(CLIENT1_ID, 1009),
                newPutRecord(CLIENT1_ID, 1010)
        ));

        assertEquals(8, appliedCount());
//        assertEquals(1, cleanQueue.size());

        assertEquals(1, flushProducer.history().size());
        FlushRequest flushReq = flushProducer.history().get(0).value();
        assertEquals(1010, flushReq.getOpsOffset());
    }

    /**
     * Retention deleting the replay position is unrecoverable (unlike during the load, where it is
     * resolved against the data topic), so it must fail with an actionable message instead of a raw
     * Kafka one.
     */
    @Test
    void testProcessOpsFailsClearlyOnRetentionLoss() {
        opsConsumer.scriptBeginningOffsets(opsPart, 100);
        opsConsumer.updateEndOffsets(singletonMap(opsPart, 200L));
        opsConsumer.seek(opsPart, 50); // Our replay position is below the live log start.

        ReplicaMapException e = assertThrows(ReplicaMapException.class, () -> opsWorker.processOps());

        assertTrue(e.getMessage().contains("retention"), "Unexpected message: " + e.getMessage());
        assertTrue(e.getMessage().contains(opsPart.toString()), "Unexpected message: " + e.getMessage());
        assertTrue(e.getCause() instanceof OffsetOutOfRangeException, "Unexpected cause: " + e.getCause());
    }

    /**
     * The time-based idle flush must be requested by the flush-partition owner only, otherwise every
     * client in the cluster sends the same request for the same idle tail.
     */
    @Test
    void testIdleFlushOnlyWhenFlushOwned() {
        FlushQueue flushQueue = flushQueues.get(opsPart.partition());

        flushQueue.initUnflushedOpsOffset(0);
        // Stale tail: added far enough in the past to exceed flushMaxUnflushed (1s, see the setup).
        flushQueue.addOpsRecord(1L, 1L, 1, true, null,
            System.nanoTime() - TimeUnit.SECONDS.toNanos(10));

        assertFalse(flushQueue.isFlushOwned());

        opsWorker.maybeRequestIdleFlush();
        assertTrue(flushProducer.history().isEmpty(), "A non-owner must not request an idle flush");

        flushQueue.setFlushOwned(true);

        opsWorker.maybeRequestIdleFlush();
        assertEquals(1, flushProducer.history().size());
        assertEquals(1, flushProducer.history().get(0).value().getOpsOffset());

        // The de-spam in staleFlushOffset keeps the owner from re-requesting the very same tail.
        opsWorker.maybeRequestIdleFlush();
        assertEquals(1, flushProducer.history().size());
    }

    /** {@code flush.max.unflushed.ms = -1} switches the time-based trigger off entirely. */
    @Test
    void testIdleFlushDisabled() {
        OpsWorker worker = newOpsWorker(dataConsumer, opsConsumer,
            Duration.ofMillis(NO_LIMIT));

        FlushQueue flushQueue = flushQueues.get(opsPart.partition());

        flushQueue.initUnflushedOpsOffset(0);
        flushQueue.addOpsRecord(1L, 1L, 1, true, null,
            System.nanoTime() - TimeUnit.SECONDS.toNanos(10)); // Stale tail.

        flushQueue.setFlushOwned(true); // Even the owner must stay silent when the trigger is off.

        worker.maybeRequestIdleFlush();

        assertTrue(flushProducer.history().isEmpty(), "A disabled trigger must never request a flush");
    }

    /**
     * {@code processOps} returns normally when its poll is woken up, thus without the {@code finally}
     * in {@code doRun} a worker stopped before it reached the steady state would leave the manager's
     * start future uncompleted forever.
     */
    @Test
    void testSteadyFutureCompletedWhenStoppedBeforeSteady() throws Exception {
        // Minimal successful load: an adopted (headerless) data topic and an empty ops topic.
        long offset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, offset));
        dataConsumer.addRecord(newDataRecord(++offset, "a", "A", null));
        dataConsumer.updateEndOffsets(singletonMap(dataPart, offset + 1));

        RealisticMockConsumer<Object,OpMessage> wokenOpsConsumer = new RealisticMockConsumer<>() {
            @Override
            public synchronized ConsumerRecords<Object,OpMessage> poll(Duration timeout) {
                throw new WakeupException(); // The stop woke the replay poll up.
            }
        };

        wokenOpsConsumer.assign(singleton(opsPart));
        wokenOpsConsumer.updateBeginningOffsets(singletonMap(opsPart, 0L));
        wokenOpsConsumer.updateEndOffsets(singletonMap(opsPart, 0L));

        OpsWorker worker = newOpsWorker(dataConsumer, wokenOpsConsumer, Duration.ofSeconds(1));

        assertFalse(worker.getSteadyFuture().isDone());

        worker.doRun(); // Returns normally: the load succeeds, then the replay poll is woken up.

        assertTrue(worker.getSteadyFuture().isCompletedExceptionally(),
            "A worker that exited before the steady state must complete its steady future");

        // Interrupt flavoured, so that the flush workers waiting for it exit quietly instead of
        // reporting an unrecoverable error for what is a clean shutdown.
        ExecutionException e = assertThrows(ExecutionException.class, () -> worker.getSteadyFuture().get());
        assertTrue(Utils.isInterrupted(e.getCause()), "Unexpected cause: " + e.getCause());
    }

    OpsWorker newOpsWorker(
        RealisticMockConsumer<Object,Object> dataConsumer,
        RealisticMockConsumer<Object,OpMessage> opsConsumer,
        Duration flushMaxUnflushed
    ) {
        return new OpsWorker(
            CLIENT1_ID, TOPIC_DATA, TOPIC_OPS, TOPIC_FLUSH,
            0,
            parts,
            dataConsumer,
            opsConsumer,
            flushProducer,
            FLUSH_MAX_OPS,
            flushMaxUnflushed,
            flushQueues,
            null,
            this::applyReceivedUpdate,
            new LongAdder(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder()
        );
    }

    /**
     * MockConsumer with the realistic fetch semantics the resume-resolution needs:
     * <ul>
     * <li>{@code auto.offset.reset=none}: polling below the beginning offset throws
     *     {@link OffsetOutOfRangeException} instead of silently resetting;</li>
     * <li>records survive polls (MockConsumer drops them after a single poll), so ranges can be
     *     re-scanned;</li>
     * <li>only records below the current end offset are served, and an empty range advances the
     *     position to the end offset the way a real fetch skips compacted/aborted gaps;</li>
     * <li>end offsets can be scripted as a sequence ({@link #scriptEndOffsets}) to emulate records
     *     appearing between reads; the last value repeats.</li>
     * </ul>
     */
    static class RealisticMockConsumer<K,V> extends MockConsumer<K,V> {
        final Map<TopicPartition,TreeMap<Long,ConsumerRecord<K,V>>> store = new HashMap<>();
        final Map<TopicPartition,Deque<Long>> endOffsetsScript = new HashMap<>();
        final Map<TopicPartition,Deque<Long>> beginningOffsetsScript = new HashMap<>();
        final Map<TopicPartition,Long> currentEnd = new HashMap<>();

        int maxPollRecords = Integer.MAX_VALUE;

        RealisticMockConsumer() {
            super(OffsetResetStrategy.NONE);
        }

        /** Real fetches are batched: limit how many records one poll serves to exercise multi-poll
         * loops. Do not combine with {@link #scriptBeginningOffsets} — the strict-reset check
         * consumes a script value per poll, so extra polls would desynchronize the script. */
        synchronized void maxPollRecords(int max) {
            maxPollRecords = max;
        }

        @Override
        public synchronized void addRecord(ConsumerRecord<K,V> rec) {
            store.computeIfAbsent(new TopicPartition(rec.topic(), rec.partition()), p -> new TreeMap<>())
                .put(rec.offset(), rec);
        }

        static Deque<Long> script(long... offsets) {
            Deque<Long> script = new ArrayDeque<>();

            for (long offset : offsets)
                script.add(offset);

            return script;
        }

        static long next(Deque<Long> script) {
            return script.size() > 1 ? script.poll() : script.peek(); // The last value repeats.
        }

        synchronized void scriptEndOffsets(TopicPartition part, long... offsets) {
            endOffsetsScript.put(part, script(offsets));
        }

        /** Values are served per {@code beginningOffsets} call (the last one repeats), emulating
         * retention advancing the log start in the middle of the resume resolution. Note the
         * strict-reset check in {@link #poll} consumes values too — scripted tests must account
         * for the exact call sequence. */
        synchronized void scriptBeginningOffsets(TopicPartition part, long... offsets) {
            beginningOffsetsScript.put(part, script(offsets));
        }

        @Override
        public synchronized void updateEndOffsets(Map<TopicPartition,Long> newOffsets) {
            for (Map.Entry<TopicPartition,Long> entry : newOffsets.entrySet())
                scriptEndOffsets(entry.getKey(), entry.getValue());
        }

        @Override
        public synchronized Map<TopicPartition,Long> endOffsets(Collection<TopicPartition> parts) {
            Map<TopicPartition,Long> result = new HashMap<>();

            for (TopicPartition part : parts) {
                Deque<Long> script = endOffsetsScript.get(part);
                assertNotNull(script, "End offsets are not set for " + part);

                long end = next(script);
                currentEnd.put(part, end);
                result.put(part, end);
            }

            return result;
        }

        @Override
        public synchronized Map<TopicPartition,Long> beginningOffsets(Collection<TopicPartition> parts) {
            Map<TopicPartition,Long> result = new HashMap<>();

            for (TopicPartition part : parts) {
                Deque<Long> script = beginningOffsetsScript.get(part);

                if (script != null)
                    result.put(part, next(script));
                else
                    result.putAll(super.beginningOffsets(singleton(part)));
            }

            return result;
        }

        @Override
        public synchronized ConsumerRecords<K,V> poll(Duration timeout) {
            Map<TopicPartition,List<ConsumerRecord<K,V>>> results = new HashMap<>();

            for (TopicPartition part : assignment()) {
                long position = position(part);

                Long beginning = beginningOffsets(singleton(part)).get(part);
                if (beginning != null && position < beginning)
                    throw new OffsetOutOfRangeException(singletonMap(part, position));

                Long end = currentEnd.get(part);
                if (end == null || position >= end)
                    continue;

                TreeMap<Long,ConsumerRecord<K,V>> recs = store.get(part);
                SortedMap<Long,ConsumerRecord<K,V>> range = recs == null ? null : recs.subMap(position, end);

                if (range == null || range.isEmpty())
                    seek(part, end); // A real fetch skips over compacted/aborted gaps.
                else {
                    List<ConsumerRecord<K,V>> served = new ArrayList<>(Math.min(range.size(), maxPollRecords));

                    for (ConsumerRecord<K,V> rec : range.values()) {
                        if (served.size() >= maxPollRecords)
                            break;

                        served.add(rec);
                    }

                    results.put(part, served);
                    seek(part, served.get(served.size() - 1).offset() + 1);
                }
            }

            return new ConsumerRecords<>(results);
        }
    }

    /** Standard flushed snapshot: 5 records "a".."e" at offsets 1001..1005 with the given header. */
    long addFlushedDataRecords(long header) {
        long offset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, offset));

        for (String key : new String[] {"a", "b", "c", "d", "e"})
            dataConsumer.addRecord(newDataRecord(++offset, key, key.toUpperCase(), header));

        long dataEnd = offset + 1; // 1006
        dataConsumer.updateEndOffsets(singletonMap(dataPart, dataEnd));
        return dataEnd;
    }

    @Test
    void testLoadDataForPartitionHealthy() {
        // The common case: the resume notification sits near the ops head, found by the first scan slice.
        long h = 1004; // The replicamap.ops header on the flushed records → the resume candidate.
        long dataEnd = addFlushedDataRecords(h);

        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 0L));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, h + 5)); // A few unflushed ops after the resume point.
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, h, dataEnd - 1, h + 1));

        dataConsumer.maxPollRecords(1); // Realistic bounded fetches: exercise the multi-poll read loop.
        opsConsumer.maxPollRecords(1);

        assertEquals(new OpsWorker.OpsResume(h + 1, h), opsWorker.loadDataForPartition(dataPart, opsPart));
        assertEquals(asList("a", "b", "c", "d", "e"), appliedKeys());
    }

    @Test
    void testLoadDataForPartitionAdopted() {
        // A pre-existing / adopted data topic: records carry no replicamap.ops header and the ops topic
        // is empty (per the README adoption steps) → replay all ops from 0, resume assertion disarmed.
        long offset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, offset));

        dataConsumer.addRecord(newDataRecord(++offset, "a", "A", null));
        dataConsumer.addRecord(newDataRecord(++offset, "b", "B", null));
        dataConsumer.addRecord(newDataRecord(++offset, "c", "C", null));

        dataConsumer.updateEndOffsets(singletonMap(dataPart, offset + 1));

        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 0L));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, 0L));

        assertEquals(new OpsWorker.OpsResume(0, -1), opsWorker.loadDataForPartition(dataPart, opsPart));
        assertEquals(asList("a", "b", "c"), appliedKeys());
    }

    @Test
    void testLoadDataForPartitionAdoptedThenFlushedDeletesCompacted() {
        // Adopted topic that was then used: all flushes were delete-only and their tombstones (the only
        // headered records) were compacted away, so the snapshot is headerless-only (candidate = -1) —
        // but the flush notifications still pin the exact resume point.
        long offset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, offset));

        dataConsumer.addRecord(newDataRecord(++offset, "a", "A", null));
        dataConsumer.addRecord(newDataRecord(++offset, "b", "B", null));
        dataConsumer.addRecord(newDataRecord(++offset, "c", "C", null));

        long dataEnd = offset + 3; // The compacted tombstones sat above the adopted records.
        dataConsumer.updateEndOffsets(singletonMap(dataPart, dataEnd));

        long opsHead = 991;
        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 0L));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, 700, dataEnd - 1, opsHead - 1));

        assertEquals(new OpsWorker.OpsResume(701, 700), opsWorker.loadDataForPartition(dataPart, opsPart));
        assertEquals(3, appliedCount());
    }

    @Test
    void testLoadDataForPartitionHeaderlessAfterHeaderedFails() {
        // Adopted (headerless) records are legal only as a prefix; a headerless record after a headered
        // one means external writes / corruption → fail the load.
        long offset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, offset));

        dataConsumer.addRecord(newDataRecord(++offset, "a", "A", 1004L)); // headered
        dataConsumer.addRecord(newDataRecord(++offset, "b", "B", null));  // headerless after headered

        dataConsumer.updateEndOffsets(singletonMap(dataPart, offset + 1));

        assertThrows(ReplicaMapException.class, () -> opsWorker.loadDataForPartition(dataPart, opsPart));
    }

    @Test
    void testLoadDataForPartitionScanFindsNotification() {
        // Candidate lags the ops head far behind (delete-only flushes whose headers were compacted
        // away): resume from the latest flush notification whose flushed data is inside our snapshot.
        long candidate = 100;
        long dataOffset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, dataOffset));

        dataConsumer.addRecord(newDataRecord(++dataOffset, "a", "A", candidate));
        dataConsumer.addRecord(newDataRecord(++dataOffset, "b", "B", candidate));

        long dataEnd = dataOffset + 1; // 1003
        dataConsumer.updateEndOffsets(singletonMap(dataPart, dataEnd));

        long opsHead = 991;
        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 0L));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, 500, dataEnd - 1, opsHead - 1));

        assertEquals(new OpsWorker.OpsResume(501, 500), opsWorker.loadDataForPartition(dataPart, opsPart));
        assertEquals(2, appliedCount());
    }

    @Test
    void testLoadDataForPartitionScanRejectsTooNewFlush() {
        // The only notification was flushed after our data load (dataOffset >= dataEnd) → reject it,
        // scan down to the floor and fall back to the exact candidate.
        long candidate = 100;
        long dataOffset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, dataOffset));

        dataConsumer.addRecord(newDataRecord(++dataOffset, "a", "A", candidate));
        long dataEnd = dataOffset + 1; // 1002
        dataConsumer.updateEndOffsets(singletonMap(dataPart, dataEnd));

        long opsHead = 991;
        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 0L));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, 500, dataEnd, opsHead - 1)); // dataOffset == dataEnd

        assertEquals(new OpsWorker.OpsResume(candidate + 1, candidate), opsWorker.loadDataForPartition(dataPart, opsPart));
    }

    @Test
    void testLoadDataForPartitionScanDoublesAcrossSlices() {
        // The resume notification sits far below the first scan window (e.g. the flushers were down for
        // a while): the backward-doubling expansion must still find it. A too-new notification near the
        // head exercises the rejection on the way.
        long candidate = 100;
        long dataOffset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, dataOffset));

        dataConsumer.addRecord(newDataRecord(++dataOffset, "a", "A", candidate));
        long dataEnd = dataOffset + 1; // 1002
        dataConsumer.updateEndOffsets(singletonMap(dataPart, dataEnd));

        long opsHead = 991; // First slice is [971, 991): the resume notification at 200 is far below it.
        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 0L));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, 190, dataEnd - 1, 200));
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, 800, dataEnd + 10, opsHead - 1)); // Too new.

        opsConsumer.maxPollRecords(1); // Realistic bounded fetches: exercise the multi-poll slice loop.

        assertEquals(new OpsWorker.OpsResume(191, 190), opsWorker.loadDataForPartition(dataPart, opsPart));
    }

    @Test
    void testLoadDataForPartitionScanAcrossDeletedRange() {
        // Retention deleted the flushed prefix the scan crosses: the out-of-range poll must continue
        // from the live log start and still find the notification. The resulting gap between the resume
        // point and the log start is then verified (stable data re-read) and skipped, with the resume
        // assertion still armed because the notification itself will be replayed.
        long candidate = 100;
        long dataOffset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, dataOffset));

        dataConsumer.addRecord(newDataRecord(++dataOffset, "a", "A", candidate));
        long dataEnd = dataOffset + 1; // 1002
        dataConsumer.updateEndOffsets(singletonMap(dataPart, dataEnd));

        long opsHead = 991;
        long opsLogStart = opsHead - 3; // 988: inside the first scan window [971, 991).
        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, opsLogStart));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, 500, dataEnd - 1, opsHead - 1));

        assertEquals(new OpsWorker.OpsResume(opsLogStart, 500), opsWorker.loadDataForPartition(dataPart, opsPart));
    }

    @Test
    void testLoadDataForPartitionIdleAllOpsDeleted() {
        // A fully-flushed partition idle past the ops retention: everything including the notifications
        // aged out (log start == head). The candidate is exact, the deleted gap is verified against the
        // stable data snapshot and skipped, the resume assertion is disarmed (its notification is gone).
        long h = 1004;
        addFlushedDataRecords(h);

        long opsHead = 2000;
        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, opsHead)); // All ops records deleted.
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));

        assertEquals(new OpsWorker.OpsResume(opsHead, -1), opsWorker.loadDataForPartition(dataPart, opsPart));
        assertEquals(5, appliedCount());
    }

    @Test
    void testLoadDataForPartitionEmptyDataWithNotification() {
        // The whole map ended deleted and all tombstones were compacted away: the data topic is empty
        // (candidate = -1) but its end offset is high, so the retained notification still qualifies and
        // pins the exact resume point.
        long dataEnd = 500;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, dataEnd)); // Empty compacted partition.
        dataConsumer.updateEndOffsets(singletonMap(dataPart, dataEnd));

        long opsHead = 991;
        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 0L));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, 700, dataEnd - 1, opsHead - 1));

        assertEquals(new OpsWorker.OpsResume(701, 700), opsWorker.loadDataForPartition(dataPart, opsPart));
        assertEquals(0, appliedCount());
    }

    @Test
    void testLoadDataForPartitionEmptyDataAllOpsDeleted() {
        // Empty compacted data topic AND all ops aged out: the empty map is the true state; the deleted
        // ops range is verified against the stable (empty) snapshot and skipped.
        long dataEnd = 500;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, dataEnd));
        dataConsumer.updateEndOffsets(singletonMap(dataPart, dataEnd));

        long opsHead = 300;
        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, opsHead));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));

        assertEquals(new OpsWorker.OpsResume(opsHead, -1), opsWorker.loadDataForPartition(dataPart, opsPart));
        assertEquals(0, appliedCount());
    }

    @Test
    void testLoadDataForPartitionNotificationDeletedTailRetained() {
        // The resume notification was retention-deleted but the unflushed tail is retained: the verified
        // skip must land exactly at the start of the tail (the log start), assertion disarmed.
        long candidate = 100;
        long dataOffset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, dataOffset));

        dataConsumer.addRecord(newDataRecord(++dataOffset, "a", "A", candidate));
        dataConsumer.updateEndOffsets(singletonMap(dataPart, dataOffset + 1));

        long opsHead = 991;
        long opsLogStart = 985; // The retained unflushed tail is [985, 991).
        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, opsLogStart));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));

        for (long offset = opsLogStart; offset < opsHead; offset++)
            opsConsumer.addRecord(newPutRecord(CLIENT2_ID, offset)); // Unflushed updates, no notifications.

        assertEquals(new OpsWorker.OpsResume(opsLogStart, -1), opsWorker.loadDataForPartition(dataPart, opsPart));
    }

    @Test
    void testLoadDataForPartitionRetriesWhenFlushRacesLoad() {
        // A flush lands after our snapshot and retention immediately deletes the ops it covered: the
        // first resolution hits the gap, the data-tail re-read picks up the racing flush's records
        // (advanced snapshot → not stable), and the retry resolves exactly via its notification.
        long dataStart = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, dataStart));

        dataConsumer.addRecord(newDataRecord(1000, "a", "A", 100L));  // Loaded by the first read.
        dataConsumer.addRecord(newDataRecord(1001, "b", "B", 700L));  // The racing flush's records:
        dataConsumer.addRecord(newDataRecord(1002, "c", "C", 700L));  // visible only to the re-read.
        dataConsumer.scriptEndOffsets(dataPart, 1001, 1001, 1003);    // First read ends at 1001, re-read at 1003.

        long opsHead = 991;
        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 600L)); // Ops below 600 deleted (flushed).
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));
        // The racing flush's notification: too new for the first snapshot (1002 >= 1001), exact for the re-read.
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, 700, 1002, opsHead - 1));

        assertEquals(new OpsWorker.OpsResume(701, 700), opsWorker.loadDataForPartition(dataPart, opsPart));

        // The continue-from-position re-read must apply exactly the racing flush's records, in order.
        assertEquals(asList("a", "b", "c"), appliedKeys());
    }

    @Test
    void testLoadDataForPartitionResolutionRetryCapExceeded() {
        // Pathological churn: every gap re-read finds the snapshot advanced again, forever. The
        // resolution must give up loudly after the retry cap instead of looping.
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, 1000L));
        dataConsumer.addRecord(newDataRecord(1000, "a", "A", 100L));

        // Initial read + one advancing re-read per attempt; each read fetches the end offset twice.
        long[] ends = new long[22];
        for (int i = 0; i < 11; i++)
            ends[2 * i] = ends[2 * i + 1] = 1001 + i;
        dataConsumer.scriptEndOffsets(dataPart, ends);

        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 900L)); // Permanent gap, nothing retained below.
        opsConsumer.updateEndOffsets(singletonMap(opsPart, 991L));

        ReplicaMapException e = assertThrows(ReplicaMapException.class,
            () -> opsWorker.loadDataForPartition(dataPart, opsPart));
        assertTrue(e.getMessage().contains("attempts"), e.getMessage());
    }

    @Test
    void testLoadDataForPartitionScanPicksLatestQualifying() {
        // Several notifications in one slice belong to our snapshot: the latest one is the resume point.
        long candidate = 100;
        long dataOffset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, dataOffset));

        dataConsumer.addRecord(newDataRecord(++dataOffset, "a", "A", candidate));
        long dataEnd = dataOffset + 1; // 1002
        dataConsumer.updateEndOffsets(singletonMap(dataPart, dataEnd));

        long opsHead = 991;
        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 0L));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, 400, dataEnd - 2, 985));
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, 500, dataEnd - 1, 990));

        assertEquals(new OpsWorker.OpsResume(501, 500), opsWorker.loadDataForPartition(dataPart, opsPart));
    }

    @Test
    void testLoadDataForPartitionRetryThenVerifiedSkip() {
        // A racing flush advances the snapshot on the first gap re-read, but its notification is not
        // retained either: the retry ends in a verified skip from the advanced candidate.
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, 1000L));
        dataConsumer.addRecord(newDataRecord(1000, "a", "A", 100L));
        dataConsumer.addRecord(newDataRecord(1001, "b", "B", 600L)); // The racing flush's record.
        dataConsumer.scriptEndOffsets(dataPart, 1001, 1001, 1002, 1002);

        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 900L)); // Everything below 900 deleted.
        opsConsumer.updateEndOffsets(singletonMap(opsPart, 991L));

        assertEquals(new OpsWorker.OpsResume(900, -1), opsWorker.loadDataForPartition(dataPart, opsPart));
        assertEquals(2, appliedCount());
    }

    @Test
    void testLoadDataForPartitionHeaderlessRevealedByRereadFails() {
        // The headerless-after-headered integrity check must hold across gap re-reads too.
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, 1000L));
        dataConsumer.addRecord(newDataRecord(1000, "a", "A", 100L)); // Headered, first read.
        dataConsumer.addRecord(newDataRecord(1001, "b", "B", null)); // Headerless, revealed by the re-read.
        dataConsumer.scriptEndOffsets(dataPart, 1001, 1001, 1002);

        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 900L)); // Forces the gap re-read.
        opsConsumer.updateEndOffsets(singletonMap(opsPart, 991L));

        assertThrows(ReplicaMapException.class, () -> opsWorker.loadDataForPartition(dataPart, opsPart));
    }

    @Test
    void testMarkSteadyWithDisarmedResume() {
        // A verified skip disarms the resume assertion: the steady state must be reachable without
        // ever replaying a flush notification.
        opsConsumer.updateEndOffsets(singletonMap(opsPart, 100L));
        opsWorker.seekOpsOffsets(singletonMap(opsPart, new OpsWorker.OpsResume(100, -1)));

        assertTrue(opsWorker.markSteady());
        assertTrue(opsWorker.getSteadyFuture().isDone());
    }

    @Test
    void testLoadDataForPartitionLogStartPassesFoundNotification() {
        // Defensive branch: the scan finds the resume notification, but retention passes its position
        // before the gap check — the skip must land on the log start with the assertion disarmed.
        // The scan result is captured to pin the branch: the scenario depends on the per-call
        // beginning-offsets script staying in sync with the code, and without the capture a call-count
        // drift could silently route this test through the not-found fallback (same expected result).
        AtomicReference<OpsWorker.FoundNotification> foundRef = new AtomicReference<>();

        OpsWorker opsWorker = new OpsWorker(
            CLIENT1_ID, TOPIC_DATA, TOPIC_OPS, TOPIC_FLUSH,
            0,
            parts,
            dataConsumer,
            opsConsumer,
            flushProducer,
            FLUSH_MAX_OPS,
            Duration.ofSeconds(1),
            flushQueues,
            null,
            this::applyReceivedUpdate,
            new LongAdder(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder()
        ) {
            @Override
            protected FoundNotification findResumeByOpsScan(TopicPartition opsPart, long opsHead, long dataEnd, long candidate) {
                FoundNotification found = super.findResumeByOpsScan(opsPart, opsHead, dataEnd, candidate);
                foundRef.set(found);
                return found;
            }
        };

        long candidate = 100;
        long dataOffset = 1000;
        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, dataOffset));

        dataConsumer.addRecord(newDataRecord(++dataOffset, "a", "A", candidate));
        long dataEnd = dataOffset + 1; // 1002
        dataConsumer.updateEndOffsets(singletonMap(dataPart, dataEnd));

        long opsHead = 991;
        opsConsumer.updateEndOffsets(singletonMap(opsPart, opsHead));
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, 500, dataEnd - 1, 990));

        // Per-call script: (1) strict check of the poll at seek 971 -> 985 -> out of range;
        // (2) the scan's log-start fetch -> 985 -> re-seek, finds the notification at 990;
        // (3) strict check of that poll -> 985; (4) the gap check -> 995: past the notification.
        opsConsumer.scriptBeginningOffsets(opsPart, 985, 985, 985, 995);

        assertEquals(new OpsWorker.OpsResume(995, -1), opsWorker.loadDataForPartition(dataPart, opsPart));

        // Pin the branch: the notification must have actually been FOUND (and then overtaken).
        OpsWorker.FoundNotification found = foundRef.get();
        assertNotNull(found, "Script drift: the scan no longer finds the notification");
        assertEquals(990, found.position);
    }

    @Test
    void testIsActuallySteady() {
        long endOffset = 100L;
        opsConsumer.updateEndOffsets(singletonMap(opsPart, endOffset));

        assertNull(opsWorker.endOffsetsOps);
        assertEquals(0L, opsWorker.maxAllowedSteadyLag);

        opsConsumer.seek(opsPart, endOffset - FLUSH_MAX_OPS - 1);
        assertFalse(opsWorker.isActuallySteady());

        assertNotNull(opsWorker.endOffsetsOps);
        assertEquals(0L, opsWorker.maxAllowedSteadyLag);

        opsConsumer.seek(opsPart, endOffset);
        opsConsumer.updateEndOffsets(singletonMap(opsPart, endOffset + FLUSH_MAX_OPS + 1));
        assertFalse(opsWorker.isActuallySteady());

        assertNotNull(opsWorker.endOffsetsOps);
        assertEquals(FLUSH_MAX_OPS, opsWorker.maxAllowedSteadyLag);

        assertFalse(opsWorker.isActuallySteady());

        assertNotNull(opsWorker.endOffsetsOps);
        assertEquals(FLUSH_MAX_OPS, opsWorker.maxAllowedSteadyLag);

        opsConsumer.seek(opsPart, endOffset + 1);
        assertTrue(opsWorker.isActuallySteady());

        assertNull(opsWorker.endOffsetsOps);
        assertEquals(FLUSH_MAX_OPS, opsWorker.maxAllowedSteadyLag);
    }

    @Test
    void testProcessOpsRecords() {
        long endOffset = 100L;
        opsConsumer.updateEndOffsets(singletonMap(opsPart, endOffset));
        opsConsumer.seek(opsPart, endOffset - FLUSH_MAX_OPS - 1);

        CompletableFuture<Void> steadyFut = opsWorker.getSteadyFuture();

        assertFalse(steadyFut.isDone());
        // Resume from flushed ops offset 48: the first replayed record is its flush notification.
        opsWorker.flushQueues.get(opsPart.partition()).initUnflushedOpsOffset(48);
        assertFalse(opsWorker.processOpsRecords(new ConsumerRecords<>(singletonMap(opsPart, asList(
            newFlushNotification(CLIENT2_ID, 48, 0, 49), // resume flush notification
            newPutRecord(CLIENT2_ID, 50),
            newPutRecord(CLIENT2_ID, 51)
        )))));

        assertEquals(2, appliedCount());
        assertFalse(steadyFut.isDone());

        assertFalse(opsWorker.processOpsRecords(new ConsumerRecords<>(emptyMap())));
        assertFalse(steadyFut.isDone());

        opsConsumer.seek(opsPart, endOffset);

        assertTrue(opsWorker.processOpsRecords(new ConsumerRecords<>(emptyMap())));
        assertTrue(steadyFut.isDone());

        assertFalse(opsWorker.processOpsRecords(new ConsumerRecords<>(emptyMap())));

        assertFalse(opsWorker.processOpsRecords(new ConsumerRecords<>(singletonMap(opsPart, asList(
            newPutRecord(CLIENT2_ID, 52),
            newPutRecord(CLIENT2_ID, 53),
            newPutRecord(CLIENT2_ID, 54),
            newPutRecord(CLIENT2_ID, 55),
            newPutRecord(CLIENT2_ID, 56),
            newPutRecord(CLIENT2_ID, 57),
            newPutRecord(CLIENT2_ID, 58),
            newPutRecord(CLIENT2_ID, 59),
            newPutRecord(CLIENT1_ID, 60),
            newPutRecord(CLIENT1_ID, 61)
        )))));

        assertEquals(12, appliedCount());

        List<ProducerRecord<Object,FlushRequest>> flushHistory = flushProducer.history();
        assertEquals(1, flushHistory.size());

        ProducerRecord<Object,FlushRequest> prepFlushReq = flushHistory.get(0);
        assertNull(prepFlushReq.key());

        FlushRequest flushReq = prepFlushReq.value();
        assertEquals(OP_FLUSH_REQUEST, flushReq.getOpType());
        assertEquals(CLIENT1_ID, flushReq.getClientId());
        assertEquals(60, flushReq.getOpsOffset());
    }

    @Test
    void testMarkSteadyChecksResumeFlushNotification() {
        FlushQueue flushQueue = opsWorker.flushQueues.get(opsPart.partition());
        flushQueue.initUnflushedOpsOffset(999); // Arm the resume offset.

        // The resume flush notification was not replayed -> markSteady must fail
        // without completing the steady future.
        assertThrows(IllegalStateException.class, opsWorker::markSteady);
        assertFalse(opsWorker.getSteadyFuture().isDone());

        // After replaying the resume flush notification markSteady succeeds.
        opsWorker.applyOpsTopicRecords(opsPart, singletonList(newFlushNotification(CLIENT2_ID, 999, 0, 1000)));
        assertTrue(opsWorker.markSteady());
        assertTrue(opsWorker.getSteadyFuture().isDone());
        assertFalse(opsWorker.markSteady()); // Already steady.
    }

    @Test
    void testLoadData() {
        // End to end over all assigned partitions: the resume point comes from the header candidate
        // confirmed by its flush notification (no consumer-group committed() call anywhere).
        long h = 999888;

        dataConsumer.updateBeginningOffsets(singletonMap(dataPart, 100L));
        dataConsumer.addRecord(newDataRecord(100L, "a", "A", h));
        dataConsumer.addRecord(newDataRecord(101L, "b", "B", h));
        dataConsumer.addRecord(newDataRecord(102L, "c", "C", h));
        dataConsumer.addRecord(newDataRecord(103L, "d", "D", h));
        dataConsumer.addRecord(newDataRecord(104L, "e", "E", h));

        dataConsumer.updateEndOffsets(singletonMap(dataPart, 105L));

        opsConsumer.updateBeginningOffsets(singletonMap(opsPart, 0L));
        opsConsumer.updateEndOffsets(singletonMap(opsPart, h + 2));
        opsConsumer.addRecord(newFlushNotification(CLIENT1_ID, h, 104, h + 1));

        // + 1 over the resume point gives the first unflushed ops position.
        assertEquals(singletonMap(opsPart, new OpsWorker.OpsResume(h + 1, h)), opsWorker.loadData());
        assertEquals(5, appliedCount());
    }
}