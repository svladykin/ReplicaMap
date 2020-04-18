package com.vladykin.replicamap.kafka.impl.part;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AllowedOnlyFlushPartitionAssignorTest {

    // ConsumerPartitionAssignor is only available in 2.4.1, so using reflection
    // to be able to compile the test with both 2.3.1 and 2.4.1 Kafka versions.
    static final Constructor<?> GRP_SUB_CONSTRUCTOR;
    static final Method GRP_SUB_UNWRAPPER;

    static {
        Constructor<?> constructor = null;
        Method unwrapper = null;
        try {
            String prefix = "org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$";

            constructor = Class.forName(prefix + "GroupSubscription")
                .getConstructor(Map.class);

            unwrapper = Class.forName(prefix + "GroupAssignment")
                .getDeclaredMethod("groupAssignment");
        }
        catch (ClassNotFoundException e) {
            // ignore
        }
        catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }

        GRP_SUB_CONSTRUCTOR = constructor;
        GRP_SUB_UNWRAPPER = unwrapper;
    }


    private static final String TOPIC = "testFlushTopic";
    private static final List<String> TOPICS_LIST = Collections.singletonList(TOPIC);
    private static final Set<String> TOPICS_SET = Collections.singleton(TOPIC);

    @Test
    void testAssignor() {
        assertEq(new short[][]{
                {0,3,4},
                {1,2}
            },
            run(5,
                createAssignor(null),
                createAssignor(new short[]{1,2})));

        assertEq(new short[][]{
                {3,5},
                {1,2}
            },
            run(7, 4,
                createAssignor(new short[]{1,3,5}),
                createAssignor(new short[]{1,2})));

        assertEq(new short[][]{
                {0,4,6},
                {3,5},
                {1,2},
            },
            run(7,
                createAssignor(null),
                createAssignor(new short[]{1,3,5}),
                createAssignor(new short[]{1,2})));

        assertEq(new short[][]{
                {6},
                {3,5},
                {1,2},
            },
            run(7, 5,
                createAssignor(new short[]{6}),
                createAssignor(new short[]{1,3,5}),
                createAssignor(new short[]{1,2})));

        assertEq(new short[][]{
                {1,5},
                {2,3}
            },
            run(7, 4,
                createAssignor(new short[]{1,3,5}),
                createAssignor(new short[]{2,3})));

        assertEq(new short[][]{
                {1,3},
                {2}
            },
            run(7, 3,
                createAssignor(new short[]{1,3}),
                createAssignor(new short[]{2,3})));
    }

    static void assertEq(short[][] exp, short[][] act) {
        assertEquals(exp.length, act.length);

        for (int i = 0; i < exp.length; i++)
            assertArrayEquals(exp[i], act[i]);
    }

    static short[][] run(int parts, AllowedOnlyFlushPartitionAssignor... assignors) {
        return run(parts, parts, assignors);
    }

    static short[][] run(int parts, int expAssignedParts, AllowedOnlyFlushPartitionAssignor... assignors) {
        Map<String,AllowedOnlyFlushPartitionAssignor.Subscription> subs = new HashMap<>();
        Set<AllowedOnlyFlushPartitionAssignor> uniqAssignors = new HashSet<>();
        String name = assignors[0].name();

        for (int i = 0; i < assignors.length; i++) {
            AllowedOnlyFlushPartitionAssignor a = assignors[i];
            assertTrue(uniqAssignors.add(a));
            assertEquals(name, a.name());
            subs.put(String.valueOf(i), new AllowedOnlyFlushPartitionAssignor.Subscription(TOPICS_LIST,
                a.subscriptionUserData(TOPICS_SET)));
        }

        List<PartitionInfo> partsInfo = new ArrayList<>();
        for (int p = 0; p < parts; p++)
            partsInfo.add(new PartitionInfo(TOPIC, p, null, null, null));

        Cluster meta = new Cluster("testCluster", Collections.emptySet(), partsInfo,
            Collections.emptySet(), Collections.emptySet());

        Map<String,AllowedOnlyFlushPartitionAssignor.Assignment> assigns = unwrap241(
            assignors[0].assign(meta, wrap241(subs)));
//            new AllowedOnlyFlushPartitionAssignor.GroupSubscription(subs)).groupAssignment(); //-- for Kafka 2.4.1

        short[][] res = new short[assigns.size()][];
        assertEquals(assignors.length, res.length);

        Set<TopicPartition> uniqParts = new HashSet<>();

        for (int i = 0; i < res.length; i++) {
            AllowedOnlyFlushPartitionAssignor.Assignment assign = assigns.get(String.valueOf(i));

            ByteBuffer userData = assign.userData();
            assertTrue(userData == null || 0 == userData.remaining());

            short[] shortAssign = new short[assign.partitions().size()];

            for (int j = 0; j < shortAssign.length; j++) {
                TopicPartition p = assign.partitions().get(j);
                assertEquals(TOPIC, p.topic());
                assertTrue(uniqParts.add(p));

                shortAssign[j] = (short)p.partition();
            }

            res[i] = shortAssign;
        }

        assertEquals(expAssignedParts, uniqParts.size());

        return res;
    }

    @SuppressWarnings("unchecked")
    static <X> X wrap241(Map<String,AllowedOnlyFlushPartitionAssignor.Subscription> subs) {
        if (GRP_SUB_CONSTRUCTOR == null)
            return (X)subs;

        try {
            return (X)GRP_SUB_CONSTRUCTOR.newInstance(subs);
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    static <X> X unwrap241 (Object subs) {
        if (GRP_SUB_UNWRAPPER == null)
            return (X)subs;

        try {
            return (X)GRP_SUB_UNWRAPPER.invoke(subs);
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static AllowedOnlyFlushPartitionAssignor createAssignor(short[] allowedParts) {
        return createAssignor(TOPIC, allowedParts);
    }

    @SuppressWarnings("SameParameterValue")
    static AllowedOnlyFlushPartitionAssignor createAssignor(String flushTopic, short[] allowedParts) {
        AllowedOnlyFlushPartitionAssignor assignor = new AllowedOnlyFlushPartitionAssignor();

        Map<String,Object> cfg = new HashMap<>();
        AllowedOnlyFlushPartitionAssignor.setupConsumerConfig(cfg, allowedParts, flushTopic);
        assignor.configure(cfg);

        assertEquals(flushTopic, assignor.flushTopic);
        assertEquals(allowedParts, assignor.allowedParts);

        return assignor;
    }
}