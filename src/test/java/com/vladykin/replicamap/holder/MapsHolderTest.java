package com.vladykin.replicamap.holder;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.TestMap;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.vladykin.replicamap.base.ReplicaMapBaseMultithreadedTest.executeThreads;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapsHolderTest {
    @Test
    void testSingle() {
        MapsHolderSingle h = new MapsHolderSingle();

        assertNull(h.ref.get());

        Object dfltId = h.getDefaultMapId();

        assertEquals(dfltId, h.getMapId(null));
        assertEquals(dfltId, h.getMapId("bla"));

        ReplicaMap<Object,Object> r = h.getMapById(dfltId, TestMap::new);
        assertSame(r, h.getMapById(dfltId, TestMap::new));

        assertThrows(IllegalArgumentException.class, () -> h.getMapById("bla", TestMap::new));

        h.close();

        assertNull(h.ref.get());
    }

    @Test
    void testMulti() {
        MapsHolderMulti h = new MapsHolderMulti() {
            @Override
            public <K> Object getMapId(K key) {
                return key instanceof String ? 1 : 2;
            }
        };

        assertThrows(UnsupportedOperationException.class, h::getDefaultMapId);

        ReplicaMap<String,Long> rs = h.getMapById(1, TestMap::new);
        ReplicaMap<Long,Long> rl = h.getMapById(2, TestMap::new);

        assertNotSame(rs, rl);
        assertSame(rs, h.getMapById(1, TestMap::new));
        assertSame(rl, h.getMapById(2, TestMap::new));
        assertEquals(2, h.maps.size());

        h.close();

        assertTrue(h.maps.isEmpty());
    }

    @Test
    void testMultithreadedSingle() throws Exception {
        MapsHolderSingle h = new MapsHolderSingle();
        doTestMultithreaded(h, h.getDefaultMapId());
    }

    @Test
    void testMultithreadedMulti() throws Exception {
        long mapId = 7;
        MapsHolderMulti h = new MapsHolderMulti() {
            @Override
            public <K> Object getMapId(K key) {
                return mapId;
            }
        };
        doTestMultithreaded(h, mapId);
    }

    void doTestMultithreaded(MapsHolder h, Object mapId) throws Exception {
        ExecutorService exec = Executors.newCachedThreadPool();

        try {
            for (int i = 0; i < 100; i++) {
                AtomicReference<ReplicaMap<?,?>> map = new AtomicReference<>();

                int threads = 10;
                CyclicBarrier start = new CyclicBarrier(threads);

                Utils.allOf(executeThreads(threads, exec, () -> {
                    start.await();

                    ReplicaMap<Object,Object> r = h.getMapById(mapId, TestMap::new);

                    if (!map.compareAndSet(null, r))
                        assertSame(r, map.get());

                    return null;
                })).get(3, TimeUnit.SECONDS);

                System.out.println("iteration " + i + " OK");
            }
        }
        finally {
            exec.shutdownNow();
            exec.awaitTermination(3, TimeUnit.SECONDS);
        }
    }
}