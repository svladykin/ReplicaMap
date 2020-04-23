package com.vladykin.replicamap.kafka.impl.part;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
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
public class AllowedOnlyPartitionAssignor extends AbstractPartitionAssignor implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(AllowedOnlyPartitionAssignor.class);

    public static final String ALLOWED_PARTS =
        AllowedOnlyPartitionAssignor.class.getSimpleName() + ".allowedParts";

    protected short[] allowedParts;
    protected byte[] allowedPartsBytes;

    @Override
    public String name() {
        return "replicamap-allowed";
    }

    @Override
    public void configure(Map<String,?> configs) {
        Object parts = configs.get(ALLOWED_PARTS);

        if (parts == null)
            return; // null means that all partitions are allowed

        allowedParts = parseAllowedPartitions(parts);
        allowedPartsBytes = Utils.serializeShortArray(allowedParts);
    }

    protected short[] parseAllowedPartitions(Object parts) {
        if (parts == null)
            return null;

        if (parts instanceof int[]) {
            int[] partsInt = (int[])parts;
            short[] partsArr = new short[partsInt.length];

            for (int i = 0; i < partsArr.length; i++)
                partsArr[i] = castAsShort(partsInt[i]);

            parts = partsArr;
        }
        if (parts instanceof long[]) {
            long[] partsLong = (long[])parts;
            short[] partsArr = new short[partsLong.length];

            for (int i = 0; i < partsArr.length; i++)
                partsArr[i] = castAsShort(partsLong[i]);

            parts = partsArr;
        }
        else {
            if (parts instanceof String) { // Comma separated list.
                String partsStr = (String)parts;
                partsStr = partsStr.trim();

                parts = partsStr.isEmpty() ?
                    Collections.emptyList() : // Compatible with Kafka.
                    Arrays.asList(Pattern.compile("\\s*,\\s*").split(partsStr, -1));
            }
            else if (parts instanceof String[])
                parts = Arrays.asList((String[])parts);
            else if (parts instanceof Number[])
                parts = Arrays.asList((Number[])parts);
            else if (parts instanceof Number)
                parts = Collections.singleton(parts);

            if (parts instanceof Collection) {
                Collection<?> partsCol = (Collection<?>)parts;
                short[] partsArr = new short[partsCol.size()];

                int i = 0;
                for (Object p : partsCol)
                    partsArr[i++] = parseAllowedPartition(p);

                parts = partsArr;
            }
        }

        if (parts instanceof short[])
            return copySortedUnique((short[])parts);

        throw new IllegalArgumentException("Failed to parse " + ALLOWED_PARTS + ": " + parts);
    }

    protected short parseAllowedPartition(Object p) {
        long x;

        if (p instanceof String)
            x = Long.parseLong(((String)p).trim());
        else if (p instanceof Number) {
            if (p instanceof BigInteger)
                x = ((BigInteger)p).longValueExact();
            else if (p instanceof BigDecimal)
                x = ((BigDecimal)p).longValueExact();
            else
                x = ((Number)p).longValue();
        }
        else
            throw new IllegalArgumentException("Failed to parse partition: " + p);

        return castAsShort(x);
    }

    protected short castAsShort(long x) {
        if (x < 0 || x > Short.MAX_VALUE)
            throw new IllegalArgumentException("Partition value is out of range: " + x);

        return (short)x;
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
    @SuppressWarnings("unused")
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

                // The score here can be interpreted as an estimated number of fair assignments
                // for a member, and actually it should be calculated as
                //       score = assignments + assignable / members
                // but to keep the code simple and performant we multiply both sides by members.
                int score = member.assignments() * members.size() + assignable;

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
