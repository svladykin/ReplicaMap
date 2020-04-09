package com.vladykin.replicamap.kafka;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.base.FailureCallback;
import com.vladykin.replicamap.holder.MapsHolder;
import com.vladykin.replicamap.kafka.compute.ComputeDeserializer;
import com.vladykin.replicamap.kafka.compute.ComputeSerializer;
import com.vladykin.replicamap.kafka.impl.FlushQueue;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.msg.OpMessageDeserializer;
import com.vladykin.replicamap.kafka.impl.msg.OpMessageSerializer;
import com.vladykin.replicamap.kafka.impl.part.AllowedOnlyFlushPartitionAssignor;
import com.vladykin.replicamap.kafka.impl.part.AllowedOnlyPartitioner;
import com.vladykin.replicamap.kafka.impl.part.NeverPartitioner;
import com.vladykin.replicamap.kafka.impl.util.Box;
import com.vladykin.replicamap.kafka.impl.util.LazyList;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import com.vladykin.replicamap.kafka.impl.worker.FlushWorker;
import com.vladykin.replicamap.kafka.impl.worker.OpsWorker;
import com.vladykin.replicamap.kafka.impl.worker.Worker;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vladykin.replicamap.kafka.KReplicaMapManager.State.NEW;
import static com.vladykin.replicamap.kafka.KReplicaMapManager.State.RUNNING;
import static com.vladykin.replicamap.kafka.KReplicaMapManager.State.STARTING;
import static com.vladykin.replicamap.kafka.KReplicaMapManager.State.STOPPED;
import static com.vladykin.replicamap.kafka.KReplicaMapManager.State.STOPPING;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.ALLOWED_PARTITIONS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.ALLOWED_PARTITIONS_RESOLVER;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.CLIENT_ID;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.COMPUTE_DESERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.COMPUTE_SERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DATA_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_FLUSH_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.DEFAULT_OPS_TOPIC_SUFFIX;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_MAX_POLL_TIMEOUT_MS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_PERIOD_OPS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_WORKERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.KEY_DESERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.KEY_SERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.MAPS_CHECK_PRECONDITION;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.MAPS_HOLDER;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_MAX_PARALLEL;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_SEND_TIMEOUT_MS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_TOPIC;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.OPS_WORKERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.PARTITIONER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.VALUE_DESERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.VALUE_SERIALIZER_CLASS;
import static com.vladykin.replicamap.kafka.impl.util.Utils.assignPartitionsRoundRobin;
import static com.vladykin.replicamap.kafka.impl.util.Utils.check;
import static com.vladykin.replicamap.kafka.impl.util.Utils.checkPositive;
import static com.vladykin.replicamap.kafka.impl.util.Utils.generateUniqueNodeId;
import static com.vladykin.replicamap.kafka.impl.util.Utils.getMacAddresses;
import static com.vladykin.replicamap.kafka.impl.util.Utils.ifNull;
import static com.vladykin.replicamap.kafka.impl.util.Utils.parseIntSet;
import static com.vladykin.replicamap.kafka.impl.util.Utils.trace;
import static java.util.stream.Collectors.toList;

/**
 * Manages all {@link ReplicaMap} instances attached to a Kafka data topic.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class KReplicaMapManager implements ReplicaMapManager {
    private static final Logger log = LoggerFactory.getLogger(KReplicaMapManager.class);

    protected static final AtomicReferenceFieldUpdater<KReplicaMapManager, State> STATE =
        AtomicReferenceFieldUpdater.newUpdater(KReplicaMapManager.class, State.class, "state");

    protected static final String SUFFIX = "_replicamap";

    protected final long clientId;
    protected final String clientIdHex;

    protected final KReplicaMapManagerConfig cfg;

    protected final String dataTopic;
    protected final String opsTopic;
    protected final String flushTopic;

    protected final Semaphore opsSemaphore;
    protected final boolean mapsCheckPrecondition;
    protected final long opsSendTimeout;
    protected final int flushPeriodOps;
    protected final long flushMaxPollTimeout;
    protected final String flushConsumerGroupId;
    protected final String dataTransactionalId;

    protected final MapsHolder maps;

    protected ComputeSerializer computeSerializer;
    protected final Producer<Object,OpMessage> opsProducer;
    protected final Producer<Object,OpMessage> flushProducer;

    protected final Queue<ConsumerRecord<Object,OpMessage>> cleanQueue;
    protected final List<FlushQueue> flushQueues;

    protected final LongAdder receivedFlushRequests = new LongAdder();
    protected final LongAdder successfulFlushes = new LongAdder();

    protected final List<FlushWorker> flushWorkers;
    protected final List<OpsWorker> opsWorkers;

    protected final CompletableFuture<ReplicaMapManager> opsSteadyFut;
    protected final CompletableFuture<ReplicaMapManager> stoppedFut = new CompletableFuture<>();

    protected final short[] allowedPartitions;

    protected volatile State state = NEW;

    /**
     * Constructor with provided config.
     *
     * @param config Configuration.
     * @see KReplicaMapManagerConfig
     */
    public KReplicaMapManager(Map<String, Object> config) {
        Utils.requireNonNull(config, "config");

        if (log.isDebugEnabled())
            log.debug("Creating {} instance.", getClass().getSimpleName());

        this.cfg = new KReplicaMapManagerConfig(config);

        dataTopic = cfg.getString(DATA_TOPIC);
        opsTopic = ifNull(cfg.getString(OPS_TOPIC), dataTopic + DEFAULT_OPS_TOPIC_SUFFIX);
        flushTopic = ifNull(cfg.getString(FLUSH_TOPIC), dataTopic + DEFAULT_FLUSH_TOPIC_SUFFIX);

        mapsCheckPrecondition = cfg.getBoolean(MAPS_CHECK_PRECONDITION);

        int maxActiveOps = cfg.getInt(OPS_MAX_PARALLEL);
        checkPositive(maxActiveOps, OPS_MAX_PARALLEL);
        opsSemaphore = new Semaphore(maxActiveOps);

        opsSendTimeout = cfg.getLong(OPS_SEND_TIMEOUT_MS);
        checkPositive(opsSendTimeout, OPS_SEND_TIMEOUT_MS);

        flushMaxPollTimeout = cfg.getLong(FLUSH_MAX_POLL_TIMEOUT_MS);
        checkPositive(flushMaxPollTimeout, FLUSH_MAX_POLL_TIMEOUT_MS);

        flushPeriodOps = cfg.getInt(FLUSH_PERIOD_OPS);
        checkPositive(flushPeriodOps, FLUSH_PERIOD_OPS);

        int opsWorkers = cfg.getInt(OPS_WORKERS);
        checkPositive(opsWorkers, OPS_WORKERS);

        int flushWorkers = cfg.getInt(FLUSH_WORKERS);
        checkPositive(flushWorkers, FLUSH_WORKERS);

        flushConsumerGroupId = flushTopic + SUFFIX;
        dataTransactionalId = dataTopic + SUFFIX;

        clientId = ifNull(cfg.getLong(CLIENT_ID), this::generateClientId);
        clientIdHex = Long.toHexString(clientId);

        allowedPartitions = resolveAllowedPartitions();

        if (allowedPartitions != null && log.isDebugEnabled()) {
            log.debug("Resolved allowed partitions for topics [{}, {}, {}]: {}",
                dataTopic, opsTopic, flushTopic, allowedPartitions);
        }

        try {
            maps = cfg.getConfiguredInstance(MAPS_HOLDER, MapsHolder.class);

            opsProducer = newKafkaProducerOps();
            int parts = resolveTotalPartitions();

            validateAllowedPartitions(parts);

            if (opsWorkers > parts)
                opsWorkers = allowedPartitions == null ? parts : allowedPartitions.length;

            if (opsWorkers > flushWorkers)
                flushWorkers = Math.max(opsWorkers >>> 1, 1);

            flushProducer = newKafkaProducerFlush();
            flushQueues = new ArrayList<>(parts);
            cleanQueue = newCleanQueue();

            for (int part = 0; part < parts; part++)
                flushQueues.add(newFlushQueue(new TopicPartition(dataTopic, part)));

            this.opsWorkers = new ArrayList<>(opsWorkers);
            for (int workerId = 0; workerId < opsWorkers; workerId++) {
                Set<Integer> assignedParts = assignPartitionsToWorker(workerId, opsWorkers, parts);
                this.opsWorkers.add(newOpsWorker(workerId, assignedParts));
            }

            opsSteadyFut = Utils.allOf(this.opsWorkers.stream()
                .map(OpsWorker::getSteadyFuture)
                .collect(toList())
            ).handle(this::onWorkersSteady);

            this.flushWorkers = new ArrayList<>(flushWorkers);
            for (int workerId = 0; workerId < flushWorkers; workerId++)
                this.flushWorkers.add(newFlushWorker(workerId, parts));

            onNewReplicaMapManager();

            if (log.isDebugEnabled()) {
                log.debug("ReplicaMap manager for topics [{}, {}, {}] is created, client id: {}",
                    dataTopic, opsTopic, flushTopic, clientIdHex);
            }
        }
        catch (Exception e) {
            Utils.close(this);
            throw new ReplicaMapException("Failed to create ReplicaMap manager for topics [" +
                dataTopic + ", " + opsTopic + ", " + flushTopic + "].", e);
        }
    }

    @SuppressWarnings("unchecked")
    protected short[] resolveAllowedPartitions() {
        Set<Integer> apSet = parseIntSet(cfg.getList(ALLOWED_PARTITIONS));

        if (apSet == null) {
            Supplier<Set<Integer>> resolver = cfg.getConfiguredInstance(ALLOWED_PARTITIONS_RESOLVER, Supplier.class);

            if (resolver != null)
                apSet = resolver.get();
        }

        if (apSet == null)
            return null;

        if (apSet.isEmpty())
            throw new ReplicaMapException("Allowed partitions list is empty.");

        apSet = new TreeSet<>(apSet); // sort the partitions
        short[] ap = new short[apSet.size()];
        int i = 0;

        for (Integer p : apSet) {
            if (p < 0 || p > Short.MAX_VALUE)
                throw new ReplicaMapException("Invalid allowed partitions: " + apSet);

            ap[i++] = (short)p.intValue();
        }

        return ap;
    }

    protected void validateAllowedPartitions(int totalParts) {
        if (allowedPartitions != null) {
            for (short part : allowedPartitions) {
                if (part >= totalParts)
                    throw new ReplicaMapException("Invalid allowed partitions: " + Arrays.toString(allowedPartitions));
            }
        }
    }

    protected int resolveTotalPartitions() {
        int opsParts = opsProducer.partitionsFor(opsTopic).size();
        int flushParts = opsProducer.partitionsFor(flushTopic).size();
        int dataParts = opsProducer.partitionsFor(dataTopic).size();

        checkEqualPartitions(opsTopic, opsParts, dataTopic, dataParts);
        checkEqualPartitions(opsTopic, opsParts, flushTopic, flushParts);

        return opsParts;
    }

    protected void checkEqualPartitions(String topic1, int parts1, String topic2, int parts2) {
        Utils.check(parts1 == parts2,
            () -> "All topics must have the same number of partitions: " +
                topic1 + "(" + parts1 + ") vs " + topic2 + "(" + parts2 + ").");
    }

    protected void onNewReplicaMapManager() {
        // no-op
    }

    protected <X> LazyList<X> newLazyList(int size) {
        return new LazyList<>(size);
    }

    protected Queue<ConsumerRecord<Object,OpMessage>> newCleanQueue() {
        return new ConcurrentLinkedQueue<>();
    }

    protected FlushQueue newFlushQueue(TopicPartition dataPart) {
        return new FlushQueue(dataPart);
    }

    protected FlushWorker newFlushWorker(int workerId, int parts) {
        if (log.isDebugEnabled())
            log.debug("Creating new flush worker {}", workerId);

        return new FlushWorker(
            clientId,
            dataTopic,
            opsTopic,
            flushTopic,
            workerId,
            flushConsumerGroupId,
            opsProducer,
            flushQueues,
            cleanQueue,
            opsSteadyFut,
            receivedFlushRequests,
            successfulFlushes,
            flushMaxPollTimeout,
            allowedPartitions,
            newLazyList(parts),
            this::newKafkaProducerData,
            newLazyList(1),
            this::newKafkaConsumerFlush
        );
    }

    protected OpsWorker newOpsWorker(int workerId, Set<Integer> assignedParts) {
        if (log.isDebugEnabled())
            log.debug("Creating new ops worker {} for partitions: {}", workerId, assignedParts);

        return new OpsWorker(
            clientId,
            dataTopic,
            opsTopic,
            flushTopic,
            workerId,
            assignedParts,
            newKafkaConsumerData(),
            newKafkaConsumerOps(),
            flushProducer,
            flushPeriodOps,
            flushQueues,
            cleanQueue,
            this::applyReceivedUpdate
        );
    }

    protected Set<Integer> assignPartitionsToWorker(int workerId, int allWorkers, int allParts) {
        return assignPartitionsRoundRobin(workerId, allWorkers, allParts, allowedPartitions);
    }

    protected long generateClientId() {
        return generateUniqueNodeId(
            System.currentTimeMillis(),
            getMacAddresses(),
            new SecureRandom()
        );
    }

    /**
     * @return Number of received flush requests.
     */
    public long getReceivedFlushRequests() {
        return receivedFlushRequests.sum();
    }

    /**
     * @return Number of successful flushes done by this manager.
     */
    public long getSuccessfulFlushes() {
        return successfulFlushes.sum();
    }

    /**
     * @return Current state of this manager.
     */
    public State getState() {
        return state;
    }

    protected boolean casState(State oldState, State newState) {
        return STATE.compareAndSet(this, oldState, newState);
    }

    protected void checkRunning() {
        check(getState() == RUNNING, () -> "Manager is not running, actual state: " + getState());
    }

    /**
     * Setup common settings for all the Kafka producers and consumers.
     * @param anyCfg Either producer or consumer config.
     */
    protected void configureAll(Map<String, Object> anyCfg) {
        anyCfg.putIfAbsent(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cfg.getList(BOOTSTRAP_SERVERS));
    }

    /**
     * Setup common settings to all the Kafka producers.
     * @param proCfg Producer config.
     */
    protected void configureAllProducers(Map<String, Object> proCfg) {
        proCfg.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, cfg.getClass(KEY_SERIALIZER_CLASS));
        proCfg.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, cfg.getClass(VALUE_SERIALIZER_CLASS));

        proCfg.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        proCfg.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        proCfg.putIfAbsent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // disallow reordering
        proCfg.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    }

    /**
     * Setup data producer.
     * @param proCfg Data producer config.
     * @param part Partition.
     */
    protected void configureProducerData(Map<String, Object> proCfg, int part) {
        proCfg.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 100L);
        proCfg.putIfAbsent(ProducerConfig.TRANSACTIONAL_ID_CONFIG, dataTransactionalId + "_" + part);
        proCfg.putIfAbsent(ProducerConfig.PARTITIONER_CLASS_CONFIG, NeverPartitioner.class);
    }

    /**
     * Setup ops producer.
     * @param proCfg Ops producer config.
     */
    @SuppressWarnings("unchecked")
    protected void configureProducerOps(Map<String, Object> proCfg) {
        proCfg.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 0L);

        Class<? extends Partitioner> partitionerClass = (Class<? extends Partitioner>)cfg.getClass(PARTITIONER_CLASS);

        if (allowedPartitions != null)
            AllowedOnlyPartitioner.setupProducerConfig(proCfg, allowedPartitions, partitionerClass);
        else
            proCfg.putIfAbsent(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerClass);
    }

    /**
     * Setup flush producer.
     * @param proCfg Flush producer config.
     */
    protected void configureProducerFlush(Map<String, Object> proCfg) {
        proCfg.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 0L);
        proCfg.putIfAbsent(ProducerConfig.PARTITIONER_CLASS_CONFIG, NeverPartitioner.class);
    }

    /**
     * Setup common settings to all the Kafka consumers.
     * @param conCfg Consumer config.
     */
    protected void configureAllConsumers(Map<String, Object> conCfg) {
        conCfg.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, cfg.getClass(KEY_DESERIALIZER_CLASS));
        conCfg.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, cfg.getClass(VALUE_DESERIALIZER_CLASS));

        conCfg.putIfAbsent(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        conCfg.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        conCfg.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        conCfg.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    }

    /**
     * Setup data consumer.
     * @param conCfg Data consumer config.
     */
    protected void configureConsumerData(@SuppressWarnings("unused") Map<String, Object> conCfg) {
        // no-op
    }

    /**
     * Setup ops consumer.
     * @param conCfg Ops consumer config.
     */
    protected void configureConsumerOps(@SuppressWarnings("unused") Map<String, Object> conCfg) {
        // no-op
    }

    /**
     * Setup flush consumer.
     * @param conCfg Flush consumer config.
     */
    protected void configureConsumerFlush(Map<String, Object> conCfg) {
        conCfg.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, flushConsumerGroupId);

        AllowedOnlyFlushPartitionAssignor.setupConsumerConfig(conCfg, allowedPartitions, flushTopic);
    }

    protected <K,V> Producer<K,V> newKafkaProducer(
        Map<String, Object> proCfg,
        Serializer<K> keySerializer,
        Serializer<V> valueSerializer
    ) {
        return new KafkaProducer<>(proCfg, keySerializer, valueSerializer);
    }

    protected Producer<Object,Object> newKafkaProducerData(int part) {
        Map<String, Object> proCfg = new TreeMap<>();

        configureAll(proCfg);
        configureAllProducers(proCfg);
        configureProducerData(proCfg, part);

        return newKafkaProducer(proCfg, null, null);
    }

    protected Producer<Object,OpMessage> newKafkaProducerOps() {
        Map<String, Object> proCfg = new TreeMap<>();

        configureAll(proCfg);
        configureAllProducers(proCfg);
        configureProducerOps(proCfg);

        return newKafkaProducer(proCfg,
            newKeySerializer(proCfg),
            newOpMessageSerializer(
                newValueSerializer(proCfg),
                updateComputeSerializer(
                    newComputeSerializer(proCfg))));
    }

    protected Producer<Object,OpMessage> newKafkaProducerFlush() {
        Map<String, Object> proCfg = new TreeMap<>();

        configureAll(proCfg);
        configureAllProducers(proCfg);
        configureProducerFlush(proCfg);

        return newKafkaProducer(proCfg,
            newKeySerializer(proCfg),
            newOpMessageSerializer(
                newValueSerializer(proCfg), null));
    }

    protected <K,V> Consumer<K,V> newKafkaConsumer(
        Map<String, Object> conCfg,
        Deserializer<K> keyDeserializer,
        Deserializer<V> valueDeserializer
    ) {
        return new KafkaConsumer<>(conCfg, keyDeserializer, valueDeserializer);
    }

    protected Consumer<Object,Object> newKafkaConsumerData() {
        Map<String, Object> conCfg = new TreeMap<>();

        configureAll(conCfg);
        configureAllConsumers(conCfg);
        configureConsumerData(conCfg);

        return newKafkaConsumer(conCfg, null, null);
    }

    protected Consumer<Object,OpMessage> newKafkaConsumerOps() {
        Map<String, Object> conCfg = new TreeMap<>();

        configureAll(conCfg);
        configureAllConsumers(conCfg);
        configureConsumerOps(conCfg);

        return newKafkaConsumer(conCfg,
            newKeyDeserializer(conCfg),
            newOpMessageDeserializer(
                newValueDeserializer(conCfg),
                newComputeDeserializer(conCfg)));
    }

    protected Consumer<Object,OpMessage> newKafkaConsumerFlush() {
        Map<String, Object> conCfg = new TreeMap<>();

        configureAll(conCfg);
        configureAllConsumers(conCfg);
        configureConsumerFlush(conCfg);

        return newKafkaConsumer(conCfg,
            newKeyDeserializer(conCfg),
            newOpMessageDeserializer(
                newValueDeserializer(conCfg), null));
    }

    @SuppressWarnings("unchecked")
    protected <K> Serializer<K> newKeySerializer(Map<String, Object> proCfg) {
        Serializer<K> s = cfg.getConfiguredInstance(KEY_SERIALIZER_CLASS, Serializer.class);
        s.configure(proCfg, true);
        return s;
    }

    @SuppressWarnings("unchecked")
    protected <V> Serializer<V> newValueSerializer(Map<String, Object> proCfg) {
        Serializer<V> s = cfg.getConfiguredInstance(VALUE_SERIALIZER_CLASS, Serializer.class);
        s.configure(proCfg, false);
        return s;
    }

    protected ComputeSerializer newComputeSerializer(Map<String, Object> proCfg) {
        ComputeSerializer s = cfg.getConfiguredInstance(COMPUTE_SERIALIZER_CLASS, ComputeSerializer.class);
        if (s != null)
            s.configure(proCfg, false);
        return s;
    }

    protected ComputeSerializer updateComputeSerializer(ComputeSerializer s) {
        if (computeSerializer != null)
            throw new IllegalStateException();

        return computeSerializer = s;
    }

    @SuppressWarnings("unchecked")
    protected <K> Deserializer<K> newKeyDeserializer(Map<String, Object> conCfg) {
        Deserializer<K> d = cfg.getConfiguredInstance(KEY_DESERIALIZER_CLASS, Deserializer.class);
        d.configure(conCfg, true);
        return d;
    }

    @SuppressWarnings("unchecked")
    protected <V> Deserializer<V> newValueDeserializer(Map<String, Object> conCfg) {
        Deserializer<V> d = cfg.getConfiguredInstance(VALUE_DESERIALIZER_CLASS, Deserializer.class);
        d.configure(conCfg, false);
        return d;
    }

    protected ComputeDeserializer newComputeDeserializer(Map<String, Object> conCfg) {
        ComputeDeserializer d = cfg.getConfiguredInstance(COMPUTE_DESERIALIZER_CLASS, ComputeDeserializer.class);
        if (d != null)
            d.configure(conCfg, false);
        return d;
    }

    protected <V> Deserializer<OpMessage> newOpMessageDeserializer(Deserializer<V> v, ComputeDeserializer c) {
        return new OpMessageDeserializer<>(v, c);
    }

    protected <V> Serializer<OpMessage> newOpMessageSerializer(Serializer<V> v, ComputeSerializer c) {
        return new OpMessageSerializer<>(v, c);
    }

    @Override
    public CompletableFuture<ReplicaMapManager> start() {
        if (!casState(NEW, STARTING))
            throw new IllegalStateException("The state is not " + NEW + ", actual state: " + getState());

        log.info("Starting for topics [{}, {}, {}], client id: {}", dataTopic, opsTopic, flushTopic, clientIdHex);

        for (OpsWorker worker : opsWorkers)
            worker.start();

        for (FlushWorker worker : flushWorkers)
            worker.start();

        return opsSteadyFut;
    }

    protected ReplicaMapManager onWorkersSteady(Void ignore, Throwable ex) {
        if (ex == null && casState(STARTING, RUNNING)) {
            log.info("Started for topics [{}, {}, {}], client id: {}", dataTopic, opsTopic, flushTopic, clientIdHex);

            return this;
        }

        Utils.close(this);
        throw new ReplicaMapException("Failed to start for topics [" +
            dataTopic + ", " + opsTopic + ", " + flushTopic + "], client id: " + clientIdHex, ex);
    }

    @Override
    public <K,V> KReplicaMap<K,V> getMap(Object mapId) {
        checkRunning();
        return getMapById(mapId);
    }

    @Override
    public <K, V> KReplicaMap<K,V> getMap() {
        checkRunning();
        return getMapById(maps.getDefaultMapId());
    }

    @Override
    public void stop() {
        for (;;) {
            State s = getState();

            if (s == STOPPED)
                return;

            if (s == STOPPING) {
                try {
                    stoppedFut.get();
                }
                catch (InterruptedException | ExecutionException e) {
                    throw new ReplicaMapException(e);
                }
                return;
            }

            if (casState(s, STOPPING))
                break;
        }

        log.info("Stopping for topics [{}, {}, {}], client id {}",
            dataTopic, opsTopic, flushTopic, clientIdHex);

        try {
            doStop();
            state = STOPPED;
            stoppedFut.complete(this);
        }
        catch (Exception e) {
            stoppedFut.completeExceptionally(e);

            throw new ReplicaMapException("Failed to stop for topics [" +
                dataTopic + ", " + opsTopic + ", " + flushTopic + "], client id: " + clientIdHex, e);
        }

        log.info("Stopped for topics [{}, {}, {}], client id: {}", dataTopic, opsTopic, flushTopic, clientIdHex);
    }

    protected void doStop() {
        Worker.interruptAll(opsWorkers);
        Worker.interruptAll(flushWorkers);

        Worker.joinAll(opsWorkers);
        Worker.joinAll(flushWorkers);

        Utils.close(opsWorkers);
        Utils.close(flushWorkers);

        Utils.close(opsProducer);
        Utils.close(flushProducer);

        Utils.close(maps);
    }

    protected <K,V> KReplicaMap<K,V> getMapById(Object mapId) {
        return (KReplicaMap<K,V>)maps.getMapById(mapId, this::newReplicaMap);
    }

    protected <K,V> KReplicaMap<K,V> newReplicaMap(Object mapId, Map<K,V> map) {
        return map instanceof NavigableMap ?
            new KReplicaNavigableMap<>(this, mapId, (NavigableMap<K,V>)map,
                opsSemaphore, mapsCheckPrecondition, opsSendTimeout, TimeUnit.MILLISECONDS) :
            new KReplicaMap<>(this, mapId, map,
                opsSemaphore, mapsCheckPrecondition, opsSendTimeout, TimeUnit.MILLISECONDS);
    }

    protected <K,V> ProducerRecord<Object,OpMessage> newOpRecord(
        @SuppressWarnings("unused") KReplicaMap<K,V> map,
        long opId,
        byte updateType,
        K key,
        V exp,
        V upd,
        BiFunction<?,?,?> function
    ) {
        return new ProducerRecord<>(opsTopic, key,
            new OpMessage(updateType, clientId, opId, exp, upd, function));
    }

    protected <K,V> void sendUpdate(
        KReplicaMap<K,V> map,
        long opId,
        byte updateType,
        K key,
        V exp,
        V upd,
        BiFunction<?,?,?> function,
        FailureCallback onSendFailed
    ) {
        checkRunning();

        if (log.isTraceEnabled()) {
            log.trace("Sending operation [{}] from map [{}] to topic [{}], for key [{}]: {} -> {}",
                (char)updateType, maps.getMapId(key), opsTopic, key, exp, upd);
        }

        opsProducer.send(newOpRecord(map, opId, updateType, key, exp, upd, function), onSendFailed);
//            (meta, err) -> {
//            if (err == null && meta != null)
//                trace.trace("sendUpdate {} offset={}, key={}, val={}", clientIdHex, meta.offset(), key, upd);
//
//            onSendFailed.onCompletion(meta, err);
//        });
    }

    @SuppressWarnings("unused")
    protected <K,V> boolean applyReceivedUpdate(
        String topic,
        int part,
        long offset,
        long clientId,
        long opId,
        byte updateType,
        K key,
        V exp,
        V upd,
        BiFunction<?,?,?> function,
        Box<V> updatedValueBox
    ) {
        trace.trace("applyReceivedUpdate {} offset={}, key={}, val={}",
            new TopicPartition(topic, part), offset, key, upd);

        Object mapId = maps.getMapId(key);

        if (log.isTraceEnabled()) {
            log.trace("Receiving operation [{}] for map [{}] from topic [{}], for key [{}]: {} -> {}",
                (char)updateType, mapId, opsTopic, key, exp, upd);
        }

        KReplicaMap<K,V> map = getMapById(mapId);

        return map.onReceiveUpdate(
            clientId == this.clientId,
            opId,
            updateType,
            key,
            exp,
            upd,
            function,
            updatedValueBox);
    }

    protected boolean canSendFunction(BiFunction<?,?,?> function) {
        ComputeSerializer s = computeSerializer;
        return s != null && s.canSerialize(function);
    }

    public enum State {
        NEW, STARTING, RUNNING, STOPPING, STOPPED
    }

    @Override
    public String toString() {
        return "KReplicaMapManager{" +
            "clientId=" + clientIdHex +
            ", dataTopic='" + dataTopic + '\'' +
            ", opsTopic='" + opsTopic + '\'' +
            ", flushTopic='" + flushTopic + '\'' +
            ", opsSemaphorePermits=" + opsSemaphore.availablePermits() +
            ", opsSemaphoreQueue=" + opsSemaphore.getQueueLength() +
            ", mapsClass=" + maps.getClass() +
            ", state=" + state +
            '}';
    }
}
