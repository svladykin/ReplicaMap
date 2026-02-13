package com.vladykin.replicamap;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static com.vladykin.replicamap.base.ReplicaMapBaseMultithreadedTest.executeThreads;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"ConstantConditions", "ArraysAsListWithZeroOrOneArgument", "unchecked"})
public class ReplicaMapTest {
    @Test
    void testMapApi() {
        doTestMapApi(new TestMap<>(new ConcurrentHashMap<>()));
    }

    private void doTestMapApi(Map<Long,Long> m) {
        assertTrue(m.isEmpty());
        assertNull(m.putIfAbsent(0L, 100L));
        assertFalse(m.isEmpty());
        assertEquals(100L, m.putIfAbsent(0L, 777L));
        assertEquals(100L, m.get(0L));
        assertEquals(1, m.size());

        assertEquals(100L, m.put(0L, 1000L));
        assertEquals(1000L, m.get(0L));

        assertEquals(1000L, m.replace(0L, 100L));
        assertEquals(100L, m.get(0L));
        assertFalse(m.replace(0L, 1000L, 2000L));
        assertEquals(100L, m.get(0L));
        assertTrue(m.replace(0L, 100L, 200L));
        assertEquals(200L, m.get(0L));

        assertTrue(m.containsKey(0L));
        assertFalse(m.containsKey(1L));
        assertTrue(m.containsValue(200L));
        assertFalse(m.containsValue(100L));

        assertEquals(Collections.singleton(0L), m.keySet());
        assertEquals(Arrays.asList(200L), new ArrayList<>(m.values()));
        Set<Map.Entry<Long,Long>> entrySet = m.entrySet();
        assertEquals(1, entrySet.size());
        int i = 0;
        for (Map.Entry<Long,Long> entry : entrySet) {
            assertEquals(0L, entry.getKey());
            assertEquals(200L, entry.getValue());
            i++;
        }
        assertEquals(1, i);

        assertFalse(m.remove(0L, 100L));
        assertTrue(m.remove(0L, 200L));
        assertTrue(m.isEmpty());
        assertEquals(0, m.size());

        HashMap<Long,Long> m1 = new HashMap<>();
        m1.put(1L, 100L);
        m1.put(2L, 200L);
        m1.put(3L, 300L);
        m.putAll(m1);
        assertEquals(3, m.size());
        for (Map.Entry<Long,Long> entry : m1.entrySet())
            assertEquals(entry.getValue(), entry.getKey() * 100);

        assertEquals(200L, m.remove(2L));
        assertEquals(2, m.size());

        m.clear();
        assertTrue(m.isEmpty());

        assertEquals(14L, m.computeIfAbsent(1L, (k) -> k + 13L));
        assertEquals(14L, m.get(1L));
        assertEquals(14L, m.computeIfAbsent(1L, (k) -> k + 15L));
        assertEquals(14L, m.get(1L));
        assertEquals(17L, m.computeIfAbsent(2L, (k) -> k + 15L));
        assertEquals(17L, m.get(2L));
        assertEquals(2, m.size());

        m.replaceAll((k,v) -> v + 1);
        assertEquals(2, m.size());
        assertEquals(15L, m.get(1L));
        assertEquals(18L, m.get(2L));

        m.clear();
        assertTrue(m.isEmpty());
    }

    @SuppressWarnings({"RedundantTypeArguments"})
    @Test
    void testNavigableMapApi() {
        TestNavigableMap<Long,Long> m = new TestNavigableMap<>(new ConcurrentSkipListMap<>(
            Comparator.<Long>naturalOrder()));
        assertSame(Comparator.naturalOrder(), m.comparator());

        doTestMapApi(m);

        m.put(1L, 100L);
        m.put(3L, 300L);
        m.put(5L, 500L);

        assertEquals(1L, m.firstEntry().getKey());
        assertEquals(100L, m.firstEntry().getValue());

        assertEquals(5L, m.lastEntry().getKey());
        assertEquals(500L, m.lastEntry().getValue());

        assertEquals(1L, m.firstKey());
        assertEquals(5L, m.lastKey());

        assertEquals(1L, m.lowerEntry(2L).getKey());
        assertEquals(100L, m.lowerEntry(2L).getValue());
        assertEquals(1L, m.lowerKey(2L));
        assertEquals(1L, m.lowerEntry(3L).getKey());
        assertEquals(100L, m.lowerEntry(3L).getValue());
        assertEquals(1L, m.lowerKey(3L));

        assertEquals(1L, m.floorEntry(2L).getKey());
        assertEquals(100L, m.floorEntry(2L).getValue());
        assertEquals(1L, m.floorKey(2L));
        assertEquals(3L, m.floorEntry(3L).getKey());
        assertEquals(300L, m.floorEntry(3L).getValue());
        assertEquals(3L, m.floorKey(3L));

        assertEquals(5L, m.higherEntry(3L).getKey());
        assertEquals(500L, m.higherEntry(3L).getValue());
        assertEquals(5L, m.higherKey(3L));
        assertEquals(5L, m.higherEntry(4L).getKey());
        assertEquals(500L, m.higherEntry(4L).getValue());
        assertEquals(5L, m.higherKey(4L));

        assertEquals(3L, m.ceilingEntry(3L).getKey());
        assertEquals(300L, m.ceilingEntry(3L).getValue());
        assertEquals(3L, m.ceilingKey(3L));
        assertEquals(5L, m.ceilingEntry(4L).getKey());
        assertEquals(500L, m.ceilingEntry(4L).getValue());
        assertEquals(5L, m.ceilingKey(4L));

        assertEquals(Arrays.asList(5L, 3L, 1L),
            new ArrayList<>(m.descendingMap().keySet()));
        assertEquals(Arrays.asList(500L, 300L, 100L),
            new ArrayList<>(m.descendingMap().values()));

        assertEquals(Arrays.asList(1L, 3L, 5L),
            new ArrayList<>(m.navigableKeySet()));
        assertEquals(Arrays.asList(5L, 3L, 1L),
            new ArrayList<>(m.descendingKeySet()));

        assertEquals(Arrays.asList(3L), new ArrayList<>(
            m.subMap(1L, false, 5L, false).keySet()));
        assertEquals(Arrays.asList(1L, 3L), new ArrayList<>(
            m.subMap(1L, true, 5L, false).keySet()));
        assertEquals(Arrays.asList(1L, 3L, 5L), new ArrayList<>(
            m.subMap(1L, true, 5L, true).keySet()));
        assertEquals(Arrays.asList(3L, 5L), new ArrayList<>(
            m.subMap(1L, false, 5L, true).keySet()));

        assertEquals(Arrays.asList(300L), new ArrayList<>(
            m.subMap(1L, false, 5L, false).values()));
        assertEquals(Arrays.asList(100L, 300L), new ArrayList<>(
            m.subMap(1L, true, 5L, false).values()));
        assertEquals(Arrays.asList(100L, 300L, 500L), new ArrayList<>(
            m.subMap(1L, true, 5L, true).values()));
        assertEquals(Arrays.asList(300L, 500L), new ArrayList<>(
            m.subMap(1L, false, 5L, true).values()));

        assertEquals(Arrays.asList(3L), new ArrayList<>(m.subMap(3L, 5L).keySet()));
        assertEquals(Arrays.asList(300L), new ArrayList<>(m.subMap(3L, 5L).values()));
        assertEquals(Arrays.asList(1L, 3L), new ArrayList<>(m.subMap(1L, 4L).keySet()));
        assertEquals(Arrays.asList(100L, 300L), new ArrayList<>(m.subMap(1L, 4L).values()));

        assertEquals(Arrays.asList(1L, 3L), new ArrayList<>(m.headMap(3L, true).keySet()));
        assertEquals(Arrays.asList(1L), new ArrayList<>(m.headMap(3L, false).keySet()));
        assertEquals(Arrays.asList(1L), new ArrayList<>(m.headMap(3L).keySet()));

        assertEquals(Arrays.asList(3L, 5L), new ArrayList<>(m.tailMap(3L, true).keySet()));
        assertEquals(Arrays.asList(5L), new ArrayList<>(m.tailMap(3L, false).keySet()));
        assertEquals(Arrays.asList(3L, 5L), new ArrayList<>(m.tailMap(3L).keySet()));

        Map.Entry<Long,Long> first = m.pollFirstEntry();
        assertEquals(1L, first.getKey());
        assertEquals(100L, first.getValue());
        assertEquals(2, m.size());

        Map.Entry<Long,Long> last = m.pollLastEntry();
        assertEquals(5L, last.getKey());
        assertEquals(500L, last.getValue());
        assertEquals(1, m.size());

        m.clear();
        assertTrue(m.isEmpty());

        assertNull(m.pollFirstEntry());
        assertNull(m.pollLastEntry());

        assertNull(m.firstEntry());
        assertNull(m.lastEntry());
    }

    @Test
    void testMultithreadedPollFirstEntry() throws Exception {
        doTestMultithreadedPollEntry(true);
    }

    @Test
    void testMultithreadedPollLastEntry() throws Exception {
        doTestMultithreadedPollEntry(false);
    }

    void doTestMultithreadedPollEntry(boolean first) throws Exception {
        TestNavigableMap<Long,Long> m = new TestNavigableMap<>(new ConcurrentSkipListMap<>(
            first ? Comparator.<Long>naturalOrder() : Comparator.<Long>reverseOrder()
        ));

        int threads = 8;
        ExecutorService exec = Executors.newFixedThreadPool(threads);

        try {
            for (int i = 0; i < 50; i++) {
                AtomicBoolean stop = new AtomicBoolean();
                CyclicBarrier start = new CyclicBarrier(threads);

                AtomicLong producedCnt = new AtomicLong();
                AtomicLong producedXor = new AtomicLong();

                AtomicLong consumedCnt = new AtomicLong();
                AtomicLong consumedXor = new AtomicLong();

                CompletableFuture<?> producers = Utils.allOf(executeThreads(threads / 2, exec, () -> {
                    start.await();

                    while (!stop.get()) {
                        long x = producedCnt.incrementAndGet();
                        assertNull(m.put(x, x));
                        for (;;) {
                            long xor = producedXor.get();
                            if (producedXor.compareAndSet(xor, xor ^ x))
                                break;
                        }
                    }

                    return null;
                }));

                CompletableFuture<?> consumers = Utils.allOf(executeThreads(threads / 2, exec, () -> {
                    start.await();

                    do {
                        Map.Entry<Long,Long> entry = first ? m.pollFirstEntry() : m.pollLastEntry();

                        if (entry != null) {
                            long x = entry.getValue();
                            for (; ; ) {
                                long xor = consumedXor.get();
                                if (consumedXor.compareAndSet(xor, xor ^ x))
                                    break;
                            }
                            consumedCnt.incrementAndGet();
                        }

                    }
                    while (consumedCnt.get() != producedCnt.get() || !producers.isDone());

                    return null;
                }));

                Thread.sleep(100);
                stop.set(true);
                CompletableFuture.allOf(producers, consumers).get(3, TimeUnit.SECONDS);

                assertEquals(producedCnt.get(), consumedCnt.get());
                assertEquals(producedXor.get(), consumedXor.get());

                System.out.println("iteration " + i + " OK");
            }
        }
        finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection", "Unchecked", "Convert2MethodRef"})
    @Test
    void testFailures() {
        Map<Long,Long> map = new ConcurrentHashMap<>();
        map.put(1L, 1L);
        map.put(2L, 2L);

        CompletableFuture failed = new CompletableFuture();
        failed.completeExceptionally(new IllegalArgumentException("test"));

        TestMap<Long,Long> m = new TestMap<Long,Long>(map) {
            @Override
            public CompletableFuture<Long> asyncPut(Long key, Long value) {
                return failed;
            }

            @Override
            public CompletableFuture<Boolean> asyncRemove(Long key, Long value) {
                return failed;
            }

            @Override
            public CompletableFuture<Boolean> asyncReplace(Long key, Long oldValue, Long newValue) {
                return failed;
            }

            @Override
            public CompletableFuture<Long> asyncPutIfAbsent(Long key, Long value) {
                return failed;
            }

            @Override
            public CompletableFuture<Long> asyncRemove(Long key) {
                return failed;
            }

            @Override
            public CompletableFuture<Long> asyncReplace(Long key, Long value) {
                return failed;
            }

            @Override
            public CompletableFuture<Long> asyncComputeIfPresent(Long key,
                BiFunction<? super Long,? super Long,? extends Long> remappingFunction) {
                return failed;
            }
        };

        assertThrows(ReplicaMapException.class, () -> m.put(0L,0L));
        assertThrows(ReplicaMapException.class, () -> m.putIfAbsent(0L,0L));
        assertThrows(ReplicaMapException.class, () -> m.replace(0L,0L));
        assertThrows(ReplicaMapException.class, () -> m.replace(0L,0L,1L));
        assertThrows(ReplicaMapException.class, () -> m.remove(0L,0L));
        assertThrows(ReplicaMapException.class, () -> m.remove(0L));
        assertThrows(ReplicaMapException.class, () -> m.putAll(Collections.singletonMap(1L,1L)));
        assertThrows(ReplicaMapException.class, () -> m.clear());
        assertThrows(ReplicaMapException.class, () -> m.putAll(Collections.singletonMap(0L,0L)));
        assertThrows(ReplicaMapException.class, () -> m.computeIfAbsent(0L, (k) -> k + 1));
        assertThrows(ReplicaMapException.class, () -> m.compute(0L, (k, v) -> k + 1));
        assertThrows(ReplicaMapException.class, () -> m.computeIfPresent(1L, (k, v) -> k + 1));
        assertThrows(ReplicaMapException.class, () -> m.merge(0L, 0L, (v1, v2) -> v1 + v2));
        assertThrows(ReplicaMapException.class, () -> m.replaceAll((k, v) -> k + 1));
    }
}
