package com.vladykin.replicamap.kafka.impl.part;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

/**
 * Partition assignor that assigns only allowed partitions.
 *
 * @author Sergei Vladykin http://vladykin.com
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

        configs.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(FlushPartitionAssignor.class));
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
