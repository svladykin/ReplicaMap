package com.vladykin.replicamap.kafka;

import com.vladykin.replicamap.holder.MapsHolderSingle;
import com.vladykin.replicamap.kafka.impl.part.KeyBytesPartitioner;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

import static java.util.Collections.emptyList;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * Configuration for {@link KReplicaMapManager}.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class KReplicaMapManagerConfig extends AbstractConfig {
    public static final String BOOTSTRAP_SERVERS = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    public static final String KEY_SERIALIZER_CLASS = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
    public static final String VALUE_SERIALIZER_CLASS = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
    public static final String KEY_DESERIALIZER_CLASS = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
    public static final String VALUE_DESERIALIZER_CLASS = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

    public static final String PARTITIONER_CLASS = ProducerConfig.PARTITIONER_CLASS_CONFIG;
    public static final String ALLOWED_PARTITIONS = "allowed.partitions";
    public static final String ALLOWED_PARTITIONS_RESOLVER = "allowed.partitions.resolver";

    public static final String COMPUTE_SERIALIZER_CLASS = "compute.serializer";
    public static final String COMPUTE_DESERIALIZER_CLASS = "compute.deserializer";

    public static final String CLIENT_ID = "client.id";
    public static final String DATA_TOPIC = "data.topic";
    public static final String OPS_TOPIC = "ops.topic";
    public static final String OPS_MAX_PARALLEL = "ops.max.parallel";
    public static final String OPS_SEND_TIMEOUT_MS = "ops.send.timeout.ms";
    public static final String OPS_WORKERS = "ops.workers";
    public static final String FLUSH_TOPIC = "flush.topic";
    public static final String FLUSH_PERIOD_OPS = "flush.period.ops";
    public static final String FLUSH_WORKERS = "flush.workers";
    public static final String MAPS_HOLDER = "maps.holder";
    public static final String MAPS_CHECK_PRECONDITION = "maps.check.precondition";

    // Defaults.
    public static final String DEFAULT_DATA_TOPIC = "replicamap";
    public static final String DEFAULT_OPS_TOPIC_SUFFIX = "_ops";
    public static final String DEFAULT_FLUSH_TOPIC_SUFFIX = "_flush";

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(CLIENT_ID, STRING, null, HIGH,
            "Unique client id, must be different for each map manager instance. " +
                "If not set, it will be generated automatically.")
        .define(DATA_TOPIC, STRING, DEFAULT_DATA_TOPIC, HIGH,
            "Kafka topic for key-value pairs.")
        .define(OPS_TOPIC, STRING, null, HIGH,
            "Kafka topic for operations.")
        .define(OPS_MAX_PARALLEL, INT, 1000, HIGH,
            "Max number of parallel operations.")
        .define(OPS_SEND_TIMEOUT_MS, LONG, 5000L, HIGH,
            "Timeout in milliseconds after which operation will fail if it was not sent yet.")
        .define(OPS_WORKERS, INT, Math.max(1, Utils.cpus() / 3), HIGH,
            "Number of worker threads processing operations from Kafka operations topic.")
        .define(FLUSH_TOPIC, STRING, null, HIGH,
            "Kafka topic for flush requests.")
        .define(FLUSH_PERIOD_OPS, INT, 3000, HIGH,
            "A number of operations after which a client should issue a flush request.")
        .define(FLUSH_WORKERS, INT, Math.max(1, Utils.cpus() / 6), HIGH,
            "Number of workers periodically flushing the updated key-value pairs to the data topic.")
        .define(MAPS_HOLDER, CLASS, MapsHolderSingle.class, HIGH,
            "Responsible for creating and holding all the replica maps for the manager and also mapping keys to their maps.")
        .define(KEY_SERIALIZER_CLASS, CLASS, StringSerializer.class, HIGH,
            "Key serializer class.")
        .define(VALUE_SERIALIZER_CLASS, CLASS, StringSerializer.class, HIGH,
            "Value serializer class.")
        .define(KEY_DESERIALIZER_CLASS, CLASS, StringDeserializer.class, HIGH,
            "Key deserializer class.")
        .define(VALUE_DESERIALIZER_CLASS, CLASS, StringDeserializer.class, HIGH,
            "Value deserializer class.")
        .define(COMPUTE_SERIALIZER_CLASS, CLASS, null, HIGH,
            "Serializer class for the functions passed to compute methods.")
        .define(COMPUTE_DESERIALIZER_CLASS, CLASS, null, HIGH,
            "Deserializer class for the functions passed to compute methods.")
        .define(BOOTSTRAP_SERVERS, LIST, emptyList(), HIGH,
            "Bootstrap Kafka servers.")
        .define(PARTITIONER_CLASS, CLASS, KeyBytesPartitioner.class, HIGH,
            "Partitioner class that implements the `org.apache.kafka.clients.producer.Partitioner` interface.")
        .define(ALLOWED_PARTITIONS, LIST, null, HIGH,
            "Set of allowed partition numbers for sharding. By default all the partitions available.")
        .define(ALLOWED_PARTITIONS_RESOLVER, CLASS, null, HIGH,
            "`Supplier<Set<Integer>>` function that resolves the set of allowed partitions.")
        .define(MAPS_CHECK_PRECONDITION, BOOLEAN, true, HIGH,
            "If true map will check preconditions locally before sending out any operations. By default is true.")
        ;

    public KReplicaMapManagerConfig(Map<?,?> originals) {
        super(CONFIG, originals);
    }
}
