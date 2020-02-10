package com.vladykin.replicamap.kafka.impl.part;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;

/**
 * {@link Partitioner} that checks for having only allowed partitions.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class AllowedOnlyPartitioner implements Partitioner {
    public static final String DELEGATE = AllowedOnlyPartitioner.class.getName() + ".delegate";
    public static final String ALLOWED_PARTS = AllowedOnlyPartitioner.class.getName() + ".allowedParts";

    protected Partitioner delegate;
    protected short[] allowedParts;

    public static void setupProducerConfig(
        Map<String,Object> configs,
        short[] allowedPartitions,
        Class<? extends Partitioner> partitionerClass
    ) {
        Utils.requireNonNull(allowedPartitions, "allowedPartitions");
        Utils.requireNonNull(partitionerClass, "partitionerClass");

        configs.putIfAbsent(ProducerConfig.PARTITIONER_CLASS_CONFIG, AllowedOnlyPartitioner.class);

        configs.putIfAbsent(AllowedOnlyPartitioner.ALLOWED_PARTS, allowedPartitions);
        configs.putIfAbsent(AllowedOnlyPartitioner.DELEGATE, partitionerClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String,?> configs) {
        allowedParts = Utils.requireNonNull((short[])configs.get(ALLOWED_PARTS), "allowedParts");

        Class<? extends Partitioner> delegateClass = (Class<? extends Partitioner>)configs.get(DELEGATE);
        delegate = Utils.getConfiguredInstance(delegateClass, configs);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int part = delegate.partition(topic, key, keyBytes, value, valueBytes, cluster);

        return checkAllowed(part, topic);
    }

    protected int checkAllowed(int part, String topic) {
        if (part >= 0 && part <= Short.MAX_VALUE && Utils.contains(allowedParts, (short)part))
            return part;

        throw new IllegalStateException("Partition " + part + " is not allowed for the topic [" + topic +
            "], allowed partitions: " + Arrays.toString(allowedParts));
    }
}
