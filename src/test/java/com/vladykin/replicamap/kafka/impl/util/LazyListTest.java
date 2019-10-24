package com.vladykin.replicamap.kafka.impl.util;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.vladykin.replicamap.base.ReplicaMapBaseMultithreadedTest.executeThreads;
import static com.vladykin.replicamap.kafka.impl.util.LazyListTest.State.CLOSED;
import static com.vladykin.replicamap.kafka.impl.util.LazyListTest.State.FAILED;
import static com.vladykin.replicamap.kafka.impl.util.LazyListTest.State.OPEN;
import static com.vladykin.replicamap.kafka.impl.util.Utils.allOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class LazyListTest {
    @Test
    void testSimple() {
        LazyList<TestCloseable> list = new LazyList<>(10);
        AtomicInteger cnt = new AtomicInteger();

        assertNull(list.get(0, null));

        list.get(1, (i) -> {
            cnt.incrementAndGet();
            return new TestCloseable();
        });
        assertEquals(1, cnt.get());

        list.get(1, (i) -> {
            cnt.incrementAndGet();
            return new TestCloseable();
        });
        assertEquals(1, cnt.get());

        TestCloseable tc = list.get(5, (i) -> {
            cnt.incrementAndGet();
            return new TestCloseable();
        });
        assertEquals(2, cnt.get());

        assertEquals(OPEN, tc.get());
        assertTrue(list.reset(5, tc));
        assertEquals(CLOSED, tc.get());

        assertNotSame(tc, list.get(5, (i) -> {
            cnt.incrementAndGet();
            return new TestCloseable();
        }));
        assertEquals(3, cnt.get());

        TestCloseable tc1 = list.get(1, null);
        TestCloseable tc5 = list.get(5, null);

        assertEquals(OPEN, tc1.get());
        assertEquals(OPEN, tc5.get());

        list.close();

        assertEquals(CLOSED, tc1.get());
        assertEquals(CLOSED, tc5.get());

        assertFalse(list.reset(1, tc1));
    }

    @Test
    void testMultithreaded() throws Exception {
        int threads = 16;
        ExecutorService exec = Executors.newFixedThreadPool(threads);

        try {
            for (int i = 0; i < 50; i++) {
                LazyList<TestCloseable> list = new LazyList<>(50);

                AtomicLong cnt = new AtomicLong();
                AtomicLong err = new AtomicLong();
                AtomicLong fail = new AtomicLong();
                AtomicLong resetFail = new AtomicLong();
                AtomicLong open = new AtomicLong();

                CyclicBarrier start = new CyclicBarrier(threads);
                AtomicBoolean stop = new AtomicBoolean();

                CompletableFuture<?> fut = allOf(executeThreads(threads, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    start.await();

                    while (!stop.get()) {
                        final int index = rnd.nextInt(list.size());
                        TestCloseable tc;

                        if (rnd.nextInt(5) != 0) {
                            try {
                                tc = list.get(index, (x) -> {
                                    if (rnd.nextInt(5) == 0) {
                                        err.incrementAndGet();
                                        throw new IllegalStateException("test");
                                    }

                                    cnt.incrementAndGet();
                                    return new TestCloseable();
                                });
                            }
                            catch (RuntimeException e) {
                                assertEquals("test", e.getCause().getMessage());
                                continue;
                            }
                            State state = tc.get();
                            if (state == FAILED) { // Reset if failed.
                                if (list.reset(index, tc))
                                    resetFail.incrementAndGet();
                            }
                            else if (state == CLOSED)
                                assertTrue(tc.wasFailed); // Was failed and then closed by reset.
                            else {
                                assertEquals(OPEN, state); // Operational.
                                open.incrementAndGet();
                            }
                        }
                        else {
                            try {
                                tc = list.get(index, null);
                            }
                            catch (RuntimeException e) {
                                assertEquals("test", e.getCause().getMessage());

                                continue;
                            }
                            if (tc != null) {
                                if (tc.setFailed())
                                    fail.incrementAndGet();
                            }
                        }
                    }

                    return null;
                }));

                Thread.sleep(100);

                stop.set(true);
                fut.get(1, SECONDS);

                assertTrue(cnt.get() > 0);
                assertTrue(resetFail.get() > 0);
                assertTrue(fail.get() > 0);
                assertTrue(err.get() > 0);
                assertTrue(open.get() > 0);

                int failRemaining = 0;
                int openRemaining = 0;
                int nullsRemaining = 0;

                for (int j = 0; j < list.size(); j++) {
                    TestCloseable tc;
                    try {
                        tc = list.get(j, null);
                    }
                    catch (Exception e) {
                        assertEquals("test", e.getCause().getCause().getMessage());
                        continue;
                    }
                    if (tc != null) {
                        if (tc.get() == OPEN)
                            openRemaining++;
                        else {
                            if (tc.get() == CLOSED)
                                assertTrue(tc.wasFailed);
                            else
                                assertEquals(FAILED, tc.get());

                            failRemaining++;
                        }
                    }
                    else
                        nullsRemaining++;
                }

                System.out.println(openRemaining + " + " + failRemaining);

                assertEquals(list.size(), openRemaining + failRemaining + nullsRemaining);

                long failRemainingExp = fail.get() - resetFail.get();
                long openRemainingExp = cnt.get() - fail.get();

//                System.out.println("failRemaining: " + failRemaining + " = " + fail + " - " + resetFail + " = " + failRemainingExp);
//                System.out.println("errRemaining:  " + errRemaining + " = " + err + " - " + resetErr + " = " + errRemainingExp);
//                System.out.println("openRemaining: " + openRemaining + " = " + cnt + " - " + fail + " = " + openRemainingExp);

                assertEquals(openRemaining, openRemainingExp);
                assertEquals(failRemaining, failRemainingExp);

                list.close();
                for (int j = 0; j < list.size(); j++) {
                    try {
                        list.get(j, null);
                        fail();
                    }
                    catch (IllegalStateException e) {
                        assertEquals("List is closed.", e.getMessage());
                    }
                }

                System.out.println("iteration " + i + " OK");
            }
        }
        finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(3, SECONDS));
        }
    }

    @Test
    void testMultithreadedCreateOnce() throws InterruptedException, ExecutionException {
        int threads = 16;
        ExecutorService exec = Executors.newFixedThreadPool(threads);

        try {
            for (int j = 0; j < 20; j++) {
                LazyList<TestCloseable> list = new LazyList<>(10);
                AtomicInteger cnt = new AtomicInteger();
                CyclicBarrier start = new CyclicBarrier(threads);

                allOf(executeThreads(threads, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    start.await();

                    for (int i = 0; i < list.size(); i++)
                        assertEquals(OPEN, list.get(rnd.nextInt(list.size()),
                            (x) -> {
                                cnt.incrementAndGet();
                                return new TestCloseable();
                            }).get());

                    return null;
                })).get();

                list.close();

                assertTrue(cnt.get() > 0);
                assertTrue(cnt.get() <= list.size());

                System.out.println("iteration " + j + " OK");
            }
        }
        finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(3, SECONDS));
        }
    }

    @BeforeEach
    void beforeEachTest() {
        TestCloseable.allInstances.clear();
    }

    @AfterEach
    void afterEachTest() {
        for (TestCloseable tc : TestCloseable.allInstances)
            assertEquals(CLOSED, tc.get());

        TestCloseable.allInstances.clear();
    }

    static class TestCloseable extends AtomicReference<State> implements AutoCloseable {
        static final Collection<TestCloseable> allInstances = new ConcurrentLinkedQueue<>();

        volatile boolean wasFailed;

        TestCloseable() {
            set(OPEN);
            allInstances.add(this);
        }

        boolean setFailed() {
            wasFailed = true;
            return compareAndSet(OPEN, FAILED);
        }

        @Override
        public void close() {
            set(CLOSED);
        }
    }

    enum State {
        OPEN, FAILED, CLOSED
    }
}
