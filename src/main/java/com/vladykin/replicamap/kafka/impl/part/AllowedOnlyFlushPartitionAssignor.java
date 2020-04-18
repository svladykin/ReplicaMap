package com.vladykin.replicamap.kafka.impl.part;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition assignor that assigns only allowed partitions.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class AllowedOnlyFlushPartitionAssignor extends AbstractPartitionAssignor implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(AllowedOnlyFlushPartitionAssignor.class);

    public static final String FLUSH_TOPIC = AllowedOnlyFlushPartitionAssignor.class.getName() + ".flushTopic";
    public static final String ALLOWED_PARTS = AllowedOnlyFlushPartitionAssignor.class.getName() + ".allowedParts";

    protected String flushTopic;
    protected short[] allowedParts;
    protected byte[] allowedPartsBytes;

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
        configs.putIfAbsent(FLUSH_TOPIC, flushTopic);

        if (allowedPartitions != null)
            configs.putIfAbsent(ALLOWED_PARTS, allowedPartitions);

        configs.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, Arrays.asList(
            AllowedOnlyFlushPartitionAssignor.class, // This one must go first to have higher priority.
            RangeAssignor.class // This is for backward compatibility with previous versions.
        ));
    }

    @Override
    public void configure(Map<String,?> configs) {
        flushTopic = (String)Utils.requireNonNull(configs.get(FLUSH_TOPIC), "flushTopic");
        allowedParts = (short[])configs.get(ALLOWED_PARTS); // null means that all partitions are allowed
        allowedPartsBytes = Utils.serializeShortArray(allowedParts);
    }

//    @Override 2.3.1
    public Subscription subscription(Set<String> topics) {
        return new Subscription(new ArrayList<>(topics), subscriptionUserData(topics));
    }

//    @Override 2.4.1
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        if (topics.size() != 1 || !topics.contains(flushTopic))
            throw new IllegalStateException("Expected flush topic: " + flushTopic + ", actual: " + topics);

        return allowedPartsBytes == null ? null : ByteBuffer.wrap(allowedPartsBytes);
    }

    @Override
    public Map<String,List<TopicPartition>> assign(
        Map<String,Integer> partitionsPerTopic,
        Map<String,Subscription> subscriptionsPerMember
    ) {
        if (partitionsPerTopic.size() != 1 || !partitionsPerTopic.containsKey(flushTopic))
            throw new IllegalStateException("Partitions per topic: " + partitionsPerTopic);

        List<Member> members = new ArrayList<>(subscriptionsPerMember.size());

        for (Map.Entry<String,Subscription> entry : subscriptionsPerMember.entrySet()) {
            short[] allowed = Utils.deserializeShortArray(entry.getValue().userData());

            members.add(new Member(entry.getKey(), allowed));
        }

        // To have stable results, maybe in the future it will be useful for incremental rebalancing.
        members.sort(Comparator.comparing(Member::id));

        int parts = partitionsPerTopic.get(flushTopic);

        for (short part = 0; part < parts; part++) {
            Member best = null;
            int bestAssignable = 0;

            for (Member member : members) {
                int assignable = member.assignable(part, parts);

                if (assignable == 0)
                    continue; // Ignore the member, can not assign this partition to it.

                if (best == null || assignable < bestAssignable ||
                    (assignable == bestAssignable && member.assignments() < best.assignments()))
                {
                    best = member;
                    bestAssignable = assignable;
                }
            }

            TopicPartition p = new TopicPartition(flushTopic, part);

            if (best != null) // Some partitions may be unassigned because they are forbidden.
                best.assign(p);
            else
                log.warn("Partition was not assigned: {}", p);
        }

        Map<String,List<TopicPartition>> result = new HashMap<>(members.size());

        for (Member member : members)
            result.put(member.id, member.assignments);

        return result;
    }

    static class Member {
        final String id;
        final short[] allowedParts; // sorted

        final List<TopicPartition> assignments = new ArrayList<>();

        Member(String id, short[] allowedParts) {
            this.id = id;
            this.allowedParts = allowedParts;
        }

        String id() {
            return id;
        }

        int assignable(short part, int parts) {
            if (allowedParts == null)
                return parts - part;

            int index = Utils.indexOf(allowedParts, part);

            if (index < 0)
                return 0;

            return allowedParts.length - index;
        }

        int assignments() {
            return assignments.size();
        }

        void assign(TopicPartition part) {
            assignments.add(part);
        }
    }
}
