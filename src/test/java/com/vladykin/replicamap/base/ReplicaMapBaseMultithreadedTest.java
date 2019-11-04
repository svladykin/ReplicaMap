package com.vladykin.replicamap.base;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapListener;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("SpellCheckingInspection")
public class ReplicaMapBaseMultithreadedTest {
    public static <T> List<CompletableFuture<T>> executeThreads(int threads, Executor exec, Callable<T> call) {
        List<CompletableFuture<T>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            CompletableFuture<T> fut = new CompletableFuture<>();

            exec.execute(() -> {
                try {
                    fut.complete(call.call());
                }
                catch (Throwable e) {
                    fut.completeExceptionally(e);

                    if (e instanceof Error)
                        throw (Error)e;

                    throw new RuntimeException(e);
                }
            });

            futures.add(fut);
        }

        return futures;
    }

    @Test
    void testReplication() throws InterruptedException {
        ExecutorService exec = Executors.newFixedThreadPool(10);
        TestMultiQueue<Integer, Integer> mq = new TestMultiQueue<>();
        AtomicLong sentOpsNum = new AtomicLong();

        Semaphore sa = new Semaphore(10);
        Semaphore sb = new Semaphore(10);
        Semaphore sc = new Semaphore(10);

        ReplicaMapMt<Integer, Integer> a = new ReplicaMapMt<>('A', mq, exec, sa, sentOpsNum);
        ReplicaMapMt<Integer, Integer> b = new ReplicaMapMt<>('B', mq, exec, sb, sentOpsNum);
        ReplicaMapMt<Integer, Integer> c = new ReplicaMapMt<>('C', mq, exec, sc, sentOpsNum);

        for (int i = 0; i < 5000; i++) {
            assertNull(a.put(1, 100));
            assertEquals(100, a.get(1));
            b.awaitForOps();
            assertEquals(100, b.get(1));
            c.awaitForOps();
            assertEquals(100, c.get(1));

            assertNull(b.putIfAbsent(2, 200));
            assertEquals(200, b.get(2));
            a.awaitForOps();
            assertEquals(200, a.get(2));
            c.awaitForOps();
            assertEquals(200, c.get(2));

            assertEquals(200, c.replace(2, 201));
            assertEquals(201, c.get(2));
            a.awaitForOps();
            assertEquals(201, a.get(2));
            b.awaitForOps();
            assertEquals(201, b.get(2));

            assertTrue(a.replace(1, 100, 101));
            assertEquals(101, a.get(1));
            b.awaitForOps();
            assertEquals(101, b.get(1));
            c.awaitForOps();
            assertEquals(101, c.get(1));

            assertEquals(201, b.remove(2));
            assertNull(b.get(2));
            a.awaitForOps();
            assertNull(a.get(2));
            c.awaitForOps();
            assertNull(c.get(2));

            assertTrue(c.remove(1, 101));
            assertNull(c.get(1));
            a.awaitForOps();
            assertNull(a.get(1));
            b.awaitForOps();
            assertNull(b.get(1));

            assertTrue(a.isEmpty());
            assertTrue(b.isEmpty());
            assertTrue(c.isEmpty());
        }

        exec.shutdownNow();
        assertTrue(exec.awaitTermination(3, TimeUnit.SECONDS));

        assertEquals(10, sa.availablePermits());
        assertEquals(10, sb.availablePermits());
        assertEquals(10, sc.availablePermits());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testMultithreadedOps() throws InterruptedException, TimeoutException, ExecutionException {
        // One of the scenarios here is when the callback was notified about the exception,
        // but the update was actually sent. Because of this we see lots warnings about
        // "Possible client id collision, AsyncOp was not found for key..."

        ExecutorService exec = Executors.newCachedThreadPool();

        try {
            TestMultiQueue<Integer,Integer> mq = new TestMultiQueue<>();
            AtomicLong sentOpsNum = new AtomicLong();

            Semaphore sa = new Semaphore(10);
            Semaphore sb = new Semaphore(10);
            Semaphore sc = new Semaphore(10);

            ReplicaMapMt<Integer,Integer> a = new ReplicaMapMt<>('A', mq, exec, sa, sentOpsNum);
            ReplicaMapMt<Integer,Integer> b = new ReplicaMapMt<>('B', mq, exec, sb, sentOpsNum);
            ReplicaMapMt<Integer,Integer> c = new ReplicaMapMt<>('C', mq, exec, sc, sentOpsNum);

            ReplicaMapMt<Integer,Integer>[] abc = new ReplicaMapMt[]{a, b, c};

            ConcurrentLinkedQueue<LogEntry> alog = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<LogEntry> blog = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<LogEntry> clog = new ConcurrentLinkedQueue<>();

            assertTrue(a.casListener(null, (map, myUpd, key, oldv, newv) -> alog.add(new LogEntry(key, oldv, newv, myUpd))));
            b.setListener((map, myUpd, key, oldv, newv) -> blog.add(new LogEntry(key, oldv, newv, myUpd)));
            ReplicaMapListener<Integer,Integer> clistener = (map, myUpd, key, oldv, newv) -> clog.add(new LogEntry(key, oldv, newv, myUpd));
            c.setListener(clistener);
            assertSame(clistener, c.getListener());

            Map<Integer,Integer> validMap = new HashMap<>();

            for (int i = 0; i < 50; i++) {
                alog.clear();
                blog.clear();
                clog.clear();

                final int threads = 37;

                AtomicBoolean stop = new AtomicBoolean();
                CyclicBarrier start = new CyclicBarrier(threads);

                CompletableFuture<?> fut = Utils.allOf(executeThreads(threads, exec, () -> {
                    Random rnd = ThreadLocalRandom.current();

                    start.await();

                    while (!stop.get()) {
                        final boolean failSend0 = rnd.nextInt(10) == 0;

                        ReplicaMapMt<Integer,Integer> map = abc[rnd.nextInt(abc.length)];

                        map.setFailNextOp(failSend0);

                        int op = rnd.nextInt(6);
                        int k = rnd.nextInt(19);
                        int v = rnd.nextInt(10);
                        Integer old;

                        try {
                            switch (op) {
                                case 0: // put
                                    map.put(k, v);
                                    break;

                                case 1: // putIfAbsent
                                    map.putIfAbsent(k, v);
                                    break;

                                case 2: // replace
                                    old = map.get(k);
                                    if (old != null)
                                        map.replace(k, old, v);
                                    break;

                                case 3: // replace if exists
                                    map.replace(k, v);
                                    break;

                                case 4: // remove
                                    map.remove(k);
                                    break;

                                case 5: // remove if equals
                                    old = map.get(k);
                                    if (old != null)
                                        map.remove(k, old);
                                    break;

                                default:
                                    fail("Op: " + op);
                            }

                            // Can not assert here because there are other resons not to send the update: preconditions, etc..
//                            assertFalse(failSend0);
                        }
                        catch (ReplicaMapException e) {
                            Throwable cause = e.getCause();
                            assertSame(ExecutionException.class, cause.getClass());

                            cause = cause.getCause();
                            assertSame(ReplicaMapException.class, cause.getClass());

                            cause = cause.getCause();

                            assertSame(TestException.class, cause.getClass());

                            assertTrue(failSend0);
                        }
                    }

                    return null;
                }));

                Thread.sleep(300);

                stop.set(true);
                fut.get(3, TimeUnit.SECONDS);

//            System.out.println("futures done");

                mq.awaitEmpty();

//            System.out.println("queue empty");

                a.awaitForOps();
                b.awaitForOps();
                c.awaitForOps();

                a.assertNoOps();
                b.assertNoOps();
                c.assertNoOps();

                assertEquals(10, sa.availablePermits());
                assertEquals(10, sb.availablePermits());
                assertEquals(10, sc.availablePermits());

//            System.out.println("ops done");

                assertEquals(a.unwrap(), b.unwrap());
                assertEquals(b.unwrap(), c.unwrap());

//            System.out.println("maps ok");

                Iterator<LogEntry> ait = alog.iterator();
                Iterator<LogEntry> bit = blog.iterator();
                Iterator<LogEntry> cit = clog.iterator();

                while (ait.hasNext()) {
                    LogEntry ax = ait.next();
                    LogEntry bx = bit.next();
                    LogEntry cx = cit.next();

                    assertEquals(ax, bx);
                    assertEquals(bx, cx);

                    // Exactly one must be an initiator.
                    assertTrue(ax.myUpd ^ bx.myUpd ^ cx.myUpd);
                }
                assertFalse(bit.hasNext());
                assertFalse(cit.hasNext());

//            System.out.println("logs + ownership ok");

                for (LogEntry entry : clog) {
                    assertEquals(entry.oldv,
                        entry.newv != null ?
                            validMap.put(entry.key, entry.newv) :
                            validMap.remove(entry.key));
                }

//            System.out.println("log replay ok");

                assertEquals(validMap, a.unwrap());

//            System.out.println("validation ok");
//            System.out.println();

                System.out.println("iteration " + i + " OK");
            }

        }
        finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    static class LogEntry {
        final int key;
        final Integer oldv;
        final Integer newv;
        final boolean myUpd;

        LogEntry(int key, Integer oldv, Integer newv, boolean myUpd) {
            this.key = key;
            this.oldv = oldv;
            this.newv = newv;
            this.myUpd = myUpd;
        }

        @Override
        public String toString() {
            return key + "=" + newv + "^" + myUpd;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LogEntry logEntry = (LogEntry)o;

            if (key != logEntry.key) return false;
            if (!Objects.equals(oldv, logEntry.oldv)) return false;
            return Objects.equals(newv, logEntry.newv);
        }

        @Override
        public int hashCode() {
            int result = key;
            result = 31 * result + (oldv != null ? oldv.hashCode() : 0);
            result = 31 * result + (newv != null ? newv.hashCode() : 0);
            return result;
        }
    }

    static class ReplicaMapMt<K,V> extends TestReplicaMapBase<K,V> {
        final AtomicLong numAppliedOps = new AtomicLong();
        final AtomicLong numSentOps;
        final Set<Long> failOps = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final ThreadLocal<Boolean> fail = new ThreadLocal<>();
        final TestMultiQueue<K,V> mq;

        ReplicaMapMt(char id, TestMultiQueue<K,V> mq, Executor exec, Semaphore maxActiveOps, AtomicLong numSentOps) {
            super(id, new ConcurrentHashMap<>(), maxActiveOps);
            this.numSentOps = numSentOps;
            this.mq = mq;
            Supplier<TestReplicaMapUpdate<K,V>> s = mq.newSupplier();
            exec.execute(() -> {
                while (!Thread.interrupted()) {
                    TestReplicaMapUpdate<K,V> upd = s.get();
                    if (upd != null) {
                        onReceiveUpdate(upd.srcId.equals(this.id), upd.opId, upd.updateType, upd.key, upd.exp, upd.upd, null, null);
                        numAppliedOps.incrementAndGet();
                    }
                }
            });
        }

        void setFailNextOp(boolean failNextOp) {
            fail.set(failNextOp);
        }

        @Override
        protected void beforeStart(AsyncOp<?,K,V> op) {
            if (fail.get() == Boolean.TRUE)
                failOps.add(op.opKey.opId);

            fail.remove();
        }

        @Override
        protected void sendUpdate(long opId, byte updateType, K key, V exp, V upd, BiFunction<?,?,?> function, FailureCallback callback) {
            if (failOps.remove(opId)) {
                if (ThreadLocalRandom.current().nextBoolean())
                    throw new TestException();

                callback.onCompletion(null, new TestException());

                // Here we test the scenario when the callback was notified about the exception, but the update was sent.
                if (ThreadLocalRandom.current().nextInt(3000) > 0)
                    return;
            }

            super.sendUpdate(opId, updateType, key, exp, upd, function, callback);
            numSentOps.incrementAndGet();
        }

        @Override
        protected void doSendUpdate(TestReplicaMapUpdate<K,V> update, FailureCallback callback) {
            mq.accept(update, callback);
        }

        void awaitForOps() throws InterruptedException {
            while (numAppliedOps.get() < numSentOps.get())
                Thread.sleep(1);
        }

        void assertNoOps() {
            assertTrue(ops.isEmpty());
        }
    }

    private static class TestException extends RuntimeException {
        // no-op
    }
}
