package com.vladykin.replicamap.kafka.impl.worker.flush;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.kafka.KReplicaMapManager;
import com.vladykin.replicamap.kafka.impl.msg.FlushNotification;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import com.vladykin.replicamap.kafka.impl.worker.Worker;
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

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

/**
 * Processes flush requests (flushes the collected updates to the `data` topic)
 * and clean requests (cleans the flush queue).
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class FlushWorker extends Worker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FlushWorker.class);

    /** Header carrying the flushed ops offset, written on every flushed data record (resume pointer on load). */
    public static final String OPS_OFFSET_HEADER = "replicamap.ops";

    protected final UUID clientId;

    protected final String dataTopic;
    protected final String opsTopic;
    protected final String flushTopic;

    protected final LazyList<Consumer<Object,FlushRequest>> flushConsumers;
    protected final Supplier<Consumer<Object,FlushRequest>> flushConsumerFactory;

    protected final LazyList<Producer<Object,Object>> dataProducers;
    protected final IntFunction<Producer<Object,Object>> dataProducerFactory;

    protected final List<FlushQueue> flushQueues;

    protected final CompletableFuture<ReplicaMapManager> opsSteadyFut;

    protected final short[] allowedPartitions;
    protected final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet(); // Concurrent only for reads, updates done by the same thread.

    protected final LongAdder receivedFlushRequests;
    protected final LongAdder successfulFlushes;

    public FlushWorker(
        UUID clientId,
        String dataTopic,
        String opsTopic,
        String flushTopic,
        int workerId,
        KReplicaMapManager manager,
        List<FlushQueue> flushQueues,
        CompletableFuture<ReplicaMapManager> opsSteadyFut,
        LongAdder receivedFlushRequests,
        LongAdder successfulFlushes,
        short[] allowedPartitions,
        LazyList<Producer<Object,Object>> dataProducers,
        IntFunction<Producer<Object,Object>> dataProducerFactory,
        LazyList<Consumer<Object,FlushRequest>> flushConsumers,
        Supplier<Consumer<Object,FlushRequest>> flushConsumerFactory
    ) {
        super("replicamap-flush-" + dataTopic + "-" + workerId + "-" + clientId, manager);

        this.clientId = clientId;
        this.dataTopic = dataTopic;
        this.opsTopic = opsTopic;
        this.flushTopic = flushTopic;
        this.flushQueues = flushQueues;
        this.opsSteadyFut = opsSteadyFut;
        this.receivedFlushRequests = receivedFlushRequests;
        this.successfulFlushes = successfulFlushes;
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
        // Need shorter time to periodically check incoming ops in the flush queues,
        // at the same time do not want to burn cpu.
        var pollTimeout = Duration.ofMillis(20);

        while (!isInterrupted())
            processFlushRequests(pollTimeout);
    }

    @Override
    protected void interruptThread() {
        Utils.wakeup(() -> flushConsumers.get(0, null));
        super.interruptThread();
    }

    protected void processFlushRequests(Duration pollTimeout) throws InterruptedException, ExecutionException {
        // We start consuming flush requests only when the ops workers are steady,
        // because otherwise we may have not enough data to flush.
        opsSteadyFut.get();

        Consumer<Object,FlushRequest> flushConsumer;
        try {
            flushConsumer = flushConsumers.get(0, this::newFlushConsumer);
        }
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Failed to create flush consumer for topic: {}", flushTopic, e);

            return;
        }

        ConsumerRecords<Object,FlushRequest> recs;
        try {
            try {
                recs = Utils.poll(flushConsumer, pollTimeout);
            }
            catch (NoOffsetForPartitionException e) {
                initFlushConsumerOffset(flushConsumer, e); // Needed when it is a new empty flush topic.
                recs = Utils.poll(flushConsumer, pollTimeout);
            }
        }
        catch (Exception e) {
            if (!Utils.isInterrupted(e))
                log.error("Failed to poll flush consumer for topic: {}", flushTopic, e);

            resetAll(flushConsumer);
            return;
        }

        for (var flushPart : assignedPartitions)
            flushPartition(flushConsumer, flushPart, recs.records(flushPart));
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

    protected void resetAll(Consumer<Object,FlushRequest> flushConsumer) {
        // Only clear ownership for the partitions this worker actually held: flushQueues is shared
        // across the manager's flush workers, and each partition is owned by exactly one of them.
        setFlushOwned(assignedPartitions, false);
        assignedPartitions.clear();

        for (int part = 0; part < dataProducers.size(); part++)
            resetDataProducer(new TopicPartition(flushTopic, part));

        flushConsumers.reset(0, flushConsumer);
    }

    protected void setFlushOwned(Collection<TopicPartition> flushPartitions, boolean owned) {
        for (TopicPartition part : flushPartitions)
            flushQueues.get(part.partition()).setFlushOwned(owned);
    }

    protected void flushPartition(
        Consumer<Object,FlushRequest> flushConsumer,
        TopicPartition flushPart,
        List<ConsumerRecord<Object, FlushRequest>> newFlushRequests
    ) {
        if (!newFlushRequests.isEmpty())
            receivedFlushRequests.add(newFlushRequests.size());

        // Here we must not modify any local state until transaction is successfully committed.
        int part = flushPart.partition();
        FlushQueue flushQueue = flushQueues.get(part);
        TopicPartition dataPart = flushQueue.getDataPartition();

        log.debug("Processing flush requests for partition {}: {}", dataPart, newFlushRequests);

        var dataBatch = flushQueue.collectBatch(newFlushRequests);
        if (dataBatch == null)
            return;

        Producer<Object,Object> dataProducer;
        long flushOffsetData;

        try {
            dataProducer = dataProducers.get(part, null);
            flushOffsetData = flushTx(dataProducer, dataBatch, flushPart, flushConsumer);
        }
        catch (Exception e) {
            boolean fenced = e instanceof ProducerFencedException;

            if (fenced || Utils.isInterrupted(e)) {
                log.warn("Failed to flush data for partition {}, flushQueueSize: {}, reason: {}",
                    dataPart, flushQueue.unflushedUpdatesSize(), Utils.getMessage(e));
            }
            else {
                log.error("Failed to flush data for partition {}, exception:", dataPart, e);
            }

            if (fenced)
                resetDataProducer(flushPart);
            else
                resetAll(flushConsumer);

            return; // No other handling, the next flush will be our retry.
        }

        if (log.isDebugEnabled()) {
            log.debug("Committed tx for partition {}, flushOffsetData: {}, dataProducer: {}, dataBatch: {}",
                dataPart, flushOffsetData, dataProducer, dataBatch);
        }

        dataBatch.commit();
        successfulFlushes.increment();
    }

    protected long flushTx(
        Producer<Object,Object> dataProducer,
        FlushQueue.Batch dataBatch,
        TopicPartition flushPart,
        Consumer<Object,FlushRequest> flushConsumer
    ) throws ExecutionException, InterruptedException {
        int part = flushPart.partition();
        Iterable<Header> opsOffsetHeader = newOpsOffsetHeader(dataBatch.opsOffset);
        Future<RecordMetadata> lastDataRecMetaFut = null;

        dataProducer.beginTransaction();
        for (Map.Entry<Object,Object> entry : dataBatch) {
            // Header on every record (not just the last): whatever survives compaction still carries the
            // ops offset, so the resume pointer is recoverable even from an all-DELETE (all-tombstone) batch.
            lastDataRecMetaFut = dataProducer.send(new ProducerRecord<>(
                dataTopic, part, entry.getKey(), entry.getValue(), opsOffsetHeader));
        }

        // The batch is never empty (collectBatch returns null otherwise), so we always have a last record.
        // Since ReplicaMap checks preconditions locally before sending anything to Kafka, there is always a
        // winner, so we can commit periodically.
        assert lastDataRecMetaFut != null;

        // The flush notification must carry the offset of the last data record, so we have to learn it
        // before sending the notification and committing. A transactional send's future completes on the
        // produce ack (the broker assigns the offset at append, before commit), so flush() + get() yields
        // the offset within the open transaction.
        dataProducer.flush();
        long flushOffsetData = lastDataRecMetaFut.get().offset();

        // Send flush notification as part of tx (opsOffset + the last data offset above).
        dataProducer.send(new ProducerRecord<>(opsTopic, part, null,
            newFlushNotification(dataBatch.opsOffset, flushOffsetData)));

        // We commit the offset of the next flush record (+1). We also (redundantly) stash the flushed ops
        // offset in the commit metadata: the resume pointer is recovered from the data-record headers and
        // flush notifications on load, but we keep the metadata for forensics / a possible future use.
        var flushConsumerOffset = new OffsetAndMetadata(
            dataBatch.getFlushRequestOffset() + 1, Long.toString(dataBatch.opsOffset));
        dataProducer.sendOffsetsToTransaction(singletonMap(flushPart, flushConsumerOffset), flushConsumer.groupMetadata());
        dataProducer.commitTransaction();

        return flushOffsetData;
    }

    public static Iterable<Header> newOpsOffsetHeader(long offset) {
        return singleton(new RecordHeader(OPS_OFFSET_HEADER, Utils.serializeVarlong(offset)));
    }

    protected FlushNotification newFlushNotification(long opsOffset, long dataOffset) {
        return new FlushNotification(clientId, opsOffset, dataOffset);
    }

    protected Producer<Object,Object> newDataProducer(int part) {
        Producer<Object,Object> p = dataProducerFactory.apply(part);

        try {
            p.initTransactions();
        }
        catch (Throwable e) {
            // The producer never made it into the LazyList (it resets the slot when the factory
            // fails), so nobody else is going to close it.
            Utils.close(p);

            throw e;
        }

        return p;
    }

    protected Consumer<Object,FlushRequest> newFlushConsumer(int ignore) {
        Consumer<Object,FlushRequest> c = flushConsumerFactory.get();

        try {
            c.subscribe(singleton(flushTopic), new PartitionRebalanceListener());
        }
        catch (Throwable e) {
            // Same as in newDataProducer: the consumer never made it into the LazyList, so nobody
            // else is going to close it.
            Utils.close(c);

            throw e;
        }

        return c;
    }

    protected void resetFlushRequests(Collection<TopicPartition> flushPartitions) {
        log.debug("Clearing unprocessed flush requests for partitions: {}", flushPartitions);

        for (TopicPartition part : flushPartitions)
            flushQueues.get(part.partition()).resetFlushRequests();
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
        return new HashSet<>(assignedPartitions);
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
            resetFlushRequests(partitions);
            initDataProducers(partitions);
            assignedPartitions.addAll(partitions);
            setFlushOwned(partitions, true);
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty())
                return;

            log.debug("Flush partitions revoked: {}", partitions);
            resetFlushRequests(partitions);
            resetDataProducers(partitions);
            setFlushOwned(partitions, false);
            assignedPartitions.removeAll(partitions);
        }
    }
}
