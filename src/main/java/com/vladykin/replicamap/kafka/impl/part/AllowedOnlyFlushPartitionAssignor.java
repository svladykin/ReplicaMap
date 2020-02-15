package com.vladykin.replicamap.kafka.impl.part;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import static java.util.Collections.emptyList;

public class AllowedOnlyFlushPartitionAssignor extends AbstractPartitionAssignor implements Configurable {

    public static final String FLUSH_TOPIC = AllowedOnlyFlushPartitionAssignor.class.getName() + ".flushTopic";
    public static final String ALLOWED_PARTS = AllowedOnlyFlushPartitionAssignor.class.getName() + ".allowedParts";

    protected String flushTopic;
    protected short[] allowedParts;
    protected byte[] allowedPartsBytes;

    @Override
    public String name() {
        return "ReplicaMap-flush";
    }

    @Override
    public void configure(Map<String,?> configs) {
        flushTopic = (String)Utils.requireNonNull(configs.get(FLUSH_TOPIC), "flushTopic");

        allowedParts = (short[])Utils.requireNonNull(configs.get(ALLOWED_PARTS), "allowedParts");
        allowedPartsBytes = Utils.serializeShortArray(allowedParts).array();
    }

    @Override
    public Subscription subscription(Set<String> topics) {
        if (topics.size() != 1 || !topics.contains(flushTopic))
            throw new IllegalStateException("Expected flush topic: " + flushTopic + ", actual: " + topics);

        return new Subscription(new ArrayList<>(topics), ByteBuffer.wrap(allowedPartsBytes));
    }

    @Override
    public Map<String,List<TopicPartition>> assign(
        Map<String,Integer> partitionsPerTopic,
        Map<String,Subscription> subscriptionsPerMember
    ) {
        if (partitionsPerTopic.size() != 1 || !partitionsPerTopic.containsKey(flushTopic))
            throw new IllegalStateException("Partitions per topic: " + partitionsPerTopic);

        int parts = partitionsPerTopic.get(flushTopic);

        if (subscriptionsPerMember == null || subscriptionsPerMember.isEmpty())
            return Collections.emptyMap();

        Map<String, short[]> allowedPartsPerMember = new HashMap<>();

        for (Map.Entry<String,Subscription> entry : subscriptionsPerMember.entrySet())
            allowedPartsPerMember.put(entry.getKey(), Utils.deserializeShortArray(entry.getValue().userData()));

        Map<String,List<TopicPartition>> result = new HashMap<>();

        PriorityQueue<String> prioritized = new PriorityQueue<>((m1, m2) -> {
            int cmp = Integer.compare(
                result.getOrDefault(m1, emptyList()).size(),
                result.getOrDefault(m2, emptyList()).size()
            );

            if (cmp == 0) {
                short[] allowedParts1 = allowedPartsPerMember.get(m1);
                short[] allowedParts2 = allowedPartsPerMember.get(m2);

                cmp = Integer.compare(
                    allowedParts1 == null ? parts : allowedParts1.length,
                    allowedParts2 == null ? parts : allowedParts2.length
                );
            }

            return cmp;
        });
        prioritized.addAll(subscriptionsPerMember.keySet());

        List<String> skipped = new ArrayList<>();

        for (int part = 0; part < parts; part++) {
            for (;;) {
                String member = prioritized.poll();

                if (member != null) { // we may end up not assigning a partition to any member
                    short[] allowed = allowedPartsPerMember.get(member);

                    // if allowed is null, then the member can accept any partitions
                    if (allowed != null && !Utils.contains(allowed, (short)part)) {
                        skipped.add(member);
                        continue;
                    }

                    List<TopicPartition> partsList = result.computeIfAbsent(member, k -> new ArrayList<>());
                    partsList.add(new TopicPartition(flushTopic, part));
                }

                if (!skipped.isEmpty()) {
                    prioritized.addAll(skipped);
                    skipped.clear();
                }
                break;
            }
        }

        return result;
    }

    @Override
    public void onAssignment(Assignment assignment) {
        super.onAssignment(assignment);

        List<TopicPartition> parts = assignment.partitions();

        if (parts == null || parts.isEmpty())
            return;

        for (TopicPartition part : parts) { // check that assigned partitions are valid
            if (!flushTopic.equals(part.topic()))
                throw new IllegalStateException("Expected flush topic: " + flushTopic + ", actual: " + part.topic());

            int p = part.partition();

            if (p < 0 || p > Short.MAX_VALUE || !Utils.contains(allowedParts, (short)p))
                throw new IllegalStateException("Invalid partition " + p + " for flush topic [" + flushTopic +
                    "] with allowed partitions: " + Arrays.toString(allowedParts));
        }
    }
}
