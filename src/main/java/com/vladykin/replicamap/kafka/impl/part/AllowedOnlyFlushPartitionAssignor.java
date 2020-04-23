package com.vladykin.replicamap.kafka.impl.part;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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

    public static final String ALLOWED_PARTS = AllowedOnlyFlushPartitionAssignor.class.getName() + ".allowedParts";

    protected short[] allowedParts;
    protected byte[] allowedPartsBytes;

    @Override
    public String name() {
        return "replicamap-flush";
    }

    public static void setupConsumerConfig(
        Map<String,Object> configs,
        short[] allowedPartitions
    ) {
        if (allowedPartitions != null)
            configs.putIfAbsent(ALLOWED_PARTS, allowedPartitions);

        configs.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, Arrays.asList(
            AllowedOnlyFlushPartitionAssignor.class, // This one must go first to have higher priority.
            RangeAssignor.class // This is for backward compatibility with previous versions.
        ));
    }

    @Override
    public void configure(Map<String,?> configs) {
        short[] parts = (short[])configs.get(ALLOWED_PARTS); // null means that all partitions are allowed

        if (parts != null) {
            allowedParts = copySortedUnique(parts);
            allowedPartsBytes = Utils.serializeShortArray(allowedParts);
        }
    }

    protected short[] copySortedUnique(short[] arr) {
        TreeSet<Short> set = new TreeSet<>();

        for (short x : arr) {
            if (x < 0)
                throw new IllegalArgumentException("Negative value: " + Arrays.toString(arr));

            set.add(x);
        }

        short[] result = new short[set.size()];

        int i = 0;
        for (short x : set)
            result[i++] = x;

        return result;
    }

    // @Override
    @SuppressWarnings("unused")
    public short version() {
        return 1;
    }

    // @Override
    public Subscription subscription(Set<String> topics) {
        return new Subscription(new ArrayList<>(topics), subscriptionUserData(topics));
    }

    // @Override
    @SuppressWarnings("unused")
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return allowedPartsBytes == null ? null : ByteBuffer.wrap(allowedPartsBytes);
    }

    @Override
    public Map<String,List<TopicPartition>> assign(
        Map<String,Integer> partitionsPerTopic,
        Map<String,Subscription> subscriptionsPerMember
    ) {
        if (new HashSet<>(partitionsPerTopic.values()).size() != 1)
            throw new IllegalStateException("Number of partitions must be the same for each topic: " + partitionsPerTopic);

        int parts = partitionsPerTopic.values().iterator().next();
        List<Member> members = new ArrayList<>(subscriptionsPerMember.size());

        for (Map.Entry<String,Subscription> entry : subscriptionsPerMember.entrySet()) {
            short[] allowed = Utils.deserializeShortArray(entry.getValue().userData());

            members.add(new Member(entry.getKey(), parts, allowed));
        }

        // To have stable results, maybe in the future it will be useful for incremental rebalancing.
        members.sort(Comparator.comparing(Member::id));

        for (short part = 0; part < parts; part++) {
            Member best = null;
            int bestScore = 0;

            for (Member member : members) {
                int assignable = member.assignable(part, parts);

                if (assignable == 0)
                    continue; // Ignore the member, can not assign this partition to it.

                int score = member.assignments() + assignable;

                if (best == null || score < bestScore) {
                    best = member;
                    bestScore = score;
                }
            }

            if (best != null) // Some partitions may be unassigned because they are forbidden for all the consumers.
                best.assign(part);
            else
                log.warn("Partition was not assigned: {}", part);
        }

        Set<String> topics = partitionsPerTopic.keySet();
        Map<String,List<TopicPartition>> result = new HashMap<>(members.size());

        for (Member member : members)
            result.put(member.id, member.fullAssignments(topics));

        return result;
    }

    static class Member {
        final String id;
        final short[] allowedParts; // sorted

        final List<Short> assignments = new ArrayList<>();

        Member(String id, int totalParts, short[] allowedParts) {
            this.id = id;
            this.allowedParts = dropExtraParts(allowedParts, totalParts); // fix misconfiguration
        }

        short[] dropExtraParts(short[] allowedParts, int totalParts) {
            if (allowedParts == null)
                return null;

            int len = allowedParts.length;

            while (len > 0 && allowedParts[len - 1] >= totalParts)
                len--; // Allowed parts are sorted, so we always peek the max value

            if (len != allowedParts.length)
                allowedParts = Arrays.copyOf(allowedParts, len);

            return allowedParts;
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

        List<TopicPartition> fullAssignments(Collection<String> topics) {
            List<TopicPartition> topicParts = new ArrayList<>(topics.size() * assignments.size());

            for (short part : assignments) {
                for (String topic : topics)
                    topicParts.add(new TopicPartition(topic, part));
            }

            return topicParts;
        }

        int assignments() {
            return assignments.size();
        }

        void assign(short part) {
            assignments.add(part);
        }
    }
}
