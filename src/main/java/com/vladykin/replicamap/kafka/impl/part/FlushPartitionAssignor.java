package com.vladykin.replicamap.kafka.impl.part;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.TopicPartition;

/**
 * Partition assignor that assigns only allowed partitions.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class FlushPartitionAssignor extends AllowedOnlyPartitionAssignor {

    protected static final String FLUSH_TOPIC = FlushPartitionAssignor.class.getSimpleName() + ".flushTopic";

    protected String flushTopic;

    @Override
    public String name() {
        return "replicamap-flush";
    }

    public static void setupConsumerConfig(
        Map<String,Object> configs,
        short[] allowedPartitions,
        String flushTopic
    ) {
        Utils.requireNonNull(flushTopic, "flushTopic");
        configs.put(FLUSH_TOPIC, flushTopic);

        if (allowedPartitions != null)
            configs.putIfAbsent(ALLOWED_PARTS, allowedPartitions);
        else
            configs.remove(ALLOWED_PARTS);

        configs.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, Arrays.asList(
            FlushPartitionAssignor.class, // This one must go first to have higher priority.
            RangeAssignor.class // This is for backward compatibility with previous versions.
        ));
    }

    @Override
    public void configure(Map<String,?> configs) {
        flushTopic = (String)Utils.requireNonNull(configs.get(FLUSH_TOPIC), FLUSH_TOPIC);

        super.configure(configs);
    }

    @Override
    public Map<String,List<TopicPartition>> assign(
        Map<String,Integer> partitionsPerTopic,
        Map<String,Subscription> subscriptionsPerMember
    ) {
        if (partitionsPerTopic.size() != 1 && !partitionsPerTopic.containsKey(flushTopic))
            throw new IllegalStateException("Flush topic expected: " + flushTopic + ", actual: " + partitionsPerTopic.keySet());

        return super.assign(partitionsPerTopic, subscriptionsPerMember);
    }
}
