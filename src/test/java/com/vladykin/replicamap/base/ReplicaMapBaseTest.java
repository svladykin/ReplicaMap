package com.vladykin.replicamap.base;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapListener;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

import static com.vladykin.replicamap.base.ReplicaMapBase.interruptRunningOps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ReplicaMapBaseTest {
    @SuppressWarnings("ConstantConditions")
    @Test
    void testSimple() throws TimeoutException, InterruptedException {
        Queue<TestReplicaMapUpdate<Integer,String>> queue = new ArrayDeque<>();
        Map<Integer, String> map = new HashMap<>();
        Semaphore maxActiveOps = new Semaphore(10);

        TestReplicaMapBase<Integer, String> rmap = new TestReplicaMapBase<Integer, String>('x', map, maxActiveOps) {
            @Override
            protected void doSendUpdate(TestReplicaMapUpdate<Integer, String> update, FailureCallback callback) {
                queue.add(update);
            }

            @Override
            protected boolean canSendFunction(BiFunction<?,?,?> function) {
                return true;
            }
        };

        System.out.println(rmap.toString());

        assertEquals('x', rmap.id());
        assertSame(map, rmap.unwrap());

        CompletableFuture<?> futPut = rmap.asyncPut(1, "one");
        assertTrue(rmap.isEmpty());
        assertEquals(9, maxActiveOps.availablePermits());
        assertFalse(futPut.isDone());

        CompletableFuture<?> futPutIfAbsent = rmap.asyncPutIfAbsent(2, "two");
        assertTrue(rmap.isEmpty());
        assertEquals(8, maxActiveOps.availablePermits());
        assertFalse(futPutIfAbsent.isDone());

        CompletableFuture<?> futPutIfAbsent2 = rmap.asyncPutIfAbsent(2, "error");
        assertTrue(rmap.isEmpty());
        assertEquals(7, maxActiveOps.availablePermits());
        assertFalse(futPutIfAbsent2.isDone());

        rmap.update(true, queue.poll());
        assertEquals(1, rmap.size());
        assertEquals("one", rmap.get(1));
        assertEquals(8, maxActiveOps.availablePermits());
        assertTrue(futPut.isDone());
        assertFalse(futPutIfAbsent.isDone());
        assertFalse(futPutIfAbsent2.isDone());

        rmap.update(true, queue.poll());
        assertEquals(2, rmap.size());
        assertEquals("one", rmap.get(1));
        assertEquals("two", rmap.get(2));
        assertEquals(9, maxActiveOps.availablePermits());
        assertTrue(futPutIfAbsent.isDone());
        assertFalse(futPutIfAbsent2.isDone());

        rmap.update(true, queue.poll());
        assertEquals(2, rmap.size());
        assertEquals("one", rmap.get(1));
        assertEquals("two", rmap.get(2));
        assertTrue(futPutIfAbsent2.isDone());

        CompletableFuture<?> futRmv = rmap.asyncRemove(1);
        assertEquals(1, queue.size());
        assertEquals(9, maxActiveOps.availablePermits());
        assertEquals(2, rmap.size());
        assertFalse(futRmv.isDone());

        rmap.update(true, queue.poll());
        assertEquals(10, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertEquals("two", rmap.get(2));
        assertTrue(futRmv.isDone());

        CompletableFuture<?> futReplace = rmap.asyncReplace(2, "twoX");
        assertEquals(1, queue.size());
        assertEquals(9, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertEquals("two", rmap.get(2));
        assertFalse(futReplace.isDone());

        rmap.update(true, queue.poll());
        assertEquals(10, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertEquals("twoX", rmap.get(2));
        assertTrue(futReplace.isDone());

        futReplace = rmap.asyncReplace(2, "twoX", "twoY");
        assertEquals(1, queue.size());
        assertEquals(9, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertEquals("twoX", rmap.get(2));
        assertFalse(futReplace.isDone());

        rmap.update(true, queue.poll());
        assertEquals(10, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertEquals("twoY", rmap.get(2));
        assertTrue(futReplace.isDone());

        futRmv = rmap.asyncRemove(2, "twoY");
        assertEquals(1, queue.size());
        assertEquals(9, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertEquals("twoY", rmap.get(2));
        assertFalse(futRmv.isDone());

        rmap.update(true, queue.poll());
        assertEquals(10, maxActiveOps.availablePermits());
        assertEquals(0, rmap.size());
        assertTrue(futRmv.isDone());

        CompletableFuture<String> futCompute = rmap.asyncComputeIfAbsent(1, String::valueOf);
        assertEquals(1, queue.size());
        assertEquals(9, maxActiveOps.availablePermits());
        assertEquals(0, rmap.size());
        assertFalse(futCompute.isDone());

        rmap.update(true, queue.poll());
        assertEquals(0, queue.size());
        assertEquals(10, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertTrue(futCompute.isDone());
        assertEquals("1", rmap.get(1));

        futCompute = rmap.asyncCompute(1, (k,v) -> k + v);
        assertEquals(1, queue.size());
        assertEquals(9, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertFalse(futCompute.isDone());

        rmap.update(true, queue.poll());
        assertEquals(0, queue.size());
        assertEquals(10, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertTrue(futCompute.isDone());
        assertEquals("11", rmap.get(1));

        futCompute = rmap.asyncComputeIfPresent(1, (k,v) -> k + v + 3);
        assertEquals(1, queue.size());
        assertEquals(9, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertFalse(futCompute.isDone());

        rmap.update(true, queue.poll());
        assertEquals(0, queue.size());
        assertEquals(10, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertTrue(futCompute.isDone());
        assertEquals("1113", rmap.get(1));

        futCompute = rmap.asyncMerge(1, "7", (v1, v2) -> v1 + v2);
        assertEquals(1, queue.size());
        assertEquals(9, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertFalse(futCompute.isDone());

        rmap.update(true, queue.poll());
        assertEquals(0, queue.size());
        assertEquals(10, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertTrue(futCompute.isDone());
        assertEquals("11137", rmap.get(1));

        futCompute = rmap.asyncMerge(0, "7", (v1, v2) -> v1 + v2);
        assertEquals(1, queue.size());
        assertEquals(9, maxActiveOps.availablePermits());
        assertEquals(1, rmap.size());
        assertFalse(futCompute.isDone());

        rmap.update(true, queue.poll());
        assertEquals(0, queue.size());
        assertEquals(10, maxActiveOps.availablePermits());
        assertEquals(2, rmap.size());
        assertTrue(futCompute.isDone());
        assertEquals("7", rmap.get(0));

        CompletableFuture<String> fut = rmap.asyncPut(10, "10");
        interruptRunningOps(rmap);
        try {
            fut.get(1, TimeUnit.SECONDS);
        }
        catch (ExecutionException e) {
            ReplicaMapException cause = (ReplicaMapException)e.getCause();
            assertSame(InterruptedException.class, cause.getCause().getClass());
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void testFailedPreconditions() throws ExecutionException, InterruptedException {
        Queue<TestReplicaMapUpdate<Integer,String>> queue = new ArrayDeque<>();
        Map<Integer, String> map = new HashMap<>();
        Semaphore maxActiveOps = new Semaphore(10);

        TestReplicaMapBase<Integer, String> rmap = new TestReplicaMapBase<Integer, String>('x', map, maxActiveOps) {
            @Override
            protected void doSendUpdate(TestReplicaMapUpdate<Integer, String> update, FailureCallback callback) {
                queue.add(update);
            }

            @Override
            protected boolean canSendFunction(BiFunction<?,?,?> function) {
                return true;
            }
        };

        String one = "one";
        CompletableFuture<String> putFut = rmap.asyncPut(1, one);
        assertFalse(putFut.isDone());
        rmap.update(true, queue.poll());
        assertNull(putFut.get());

        String anotherOne = "ONE".toLowerCase();
        assertNotSame(one, anotherOne);
        putFut = rmap.asyncPut(1, anotherOne);
        assertTrue(putFut.isDone()); // if value is equal to an existing one precondition must fail
        assertSame(one, putFut.get());
        assertSame(one, rmap.get(1));

        assertEquals("one", assertDone(rmap.asyncPutIfAbsent(1, "two")).get());

        assertFalse(assertDone(rmap.asyncRemove(1, "two")).get());
        assertNull(assertDone(rmap.asyncRemove(2)).get());

        assertNull(assertDone(rmap.asyncReplace(2, "two")).get());
        assertFalse(assertDone(rmap.asyncReplace(1, "two", "five")).get());

        assertEquals("one", assertDone(rmap.asyncComputeIfAbsent(1, (k) -> String.valueOf(k + 1))).get());
        assertNull(assertDone(rmap.asyncComputeIfPresent(100, (k, v) -> k + v)).get());

        assertEquals(10, maxActiveOps.availablePermits());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void testDisabledPreconditions() throws ExecutionException, InterruptedException {
        Queue<TestReplicaMapUpdate<Integer,String>> queue = new ArrayDeque<>();
        Map<Integer, String> map = new HashMap<>();
        Semaphore maxActiveOps = new Semaphore(10);

        TestReplicaMapBase<Integer, String> rmap = new TestReplicaMapBase<Integer, String>(
            'x', map, maxActiveOps, false, Long.MAX_VALUE, TimeUnit.NANOSECONDS
        ) {
            @Override
            protected void doSendUpdate(TestReplicaMapUpdate<Integer, String> update, FailureCallback callback) {
                queue.add(update);
            }

            @Override
            protected boolean canSendFunction(BiFunction<?,?,?> function) {
                return true;
            }
        };

        String one = "one";
        CompletableFuture<String> putFut = rmap.asyncPut(1, one);
        assertFalse(putFut.isDone());
        rmap.update(true, queue.poll());
        assertNull(putFut.get());

        String anotherOne = "ONE".toLowerCase();
        assertNotSame(one, anotherOne);
        putFut = rmap.asyncPut(1, anotherOne);
        assertFalse(putFut.isDone());
        rmap.update(true, queue.poll());
        assertSame(one, putFut.get());
        assertSame(anotherOne, rmap.get(1));

        int queueSize = 0;
        assertTrue(queue.isEmpty());
        assertFalse(rmap.asyncPutIfAbsent(1, "two").isDone());
        assertEquals(++queueSize, queue.size());

        assertFalse(rmap.asyncRemove(1, "two").isDone());
        assertEquals(++queueSize, queue.size());

        assertFalse(rmap.asyncRemove(2).isDone());
        assertEquals(++queueSize, queue.size());

        assertFalse(rmap.asyncReplace(2, "two").isDone());
        assertEquals(++queueSize, queue.size());

        assertFalse(rmap.asyncReplace(1, "two", "five").isDone());
        assertEquals(++queueSize, queue.size());

        assertFalse(rmap.asyncComputeIfAbsent(1, (k) -> String.valueOf(k + 1)).isDone());
        assertEquals(++queueSize, queue.size());

        assertFalse(rmap.asyncComputeIfPresent(100, (k, v) -> k + v).isDone());
        assertEquals(++queueSize, queue.size());

        assertEquals(10 - queueSize, maxActiveOps.availablePermits());
    }

    @Test
    void testSendTimeoutOnAcquirePermit() throws InterruptedException {
        TestReplicaMapBase<Integer, String> rmap = new TestReplicaMapBase<Integer, String>('x', new HashMap<>(),
            new Semaphore(0), true, 1, TimeUnit.MILLISECONDS) {
            @Override
            protected void doSendUpdate(TestReplicaMapUpdate<Integer, String> update, FailureCallback callback) {
                throw new  IllegalStateException();
            }
        };

        try {
            rmap.asyncPut(1, "one").get();
            fail("Exception expected.");
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertSame(ReplicaMapException.class, cause.getClass());

            cause = cause.getCause();
            assertSame(TimeoutException.class, cause.getClass());
        }
    }

    @Test
    void testSendFailure() throws InterruptedException {
        Semaphore s = new Semaphore(10);
        TestReplicaMapBase<Integer, String> rmap = new TestReplicaMapBase<Integer, String>('x', new HashMap<>(),
            new Semaphore(10)) {
            @Override
            protected void doSendUpdate(TestReplicaMapUpdate<Integer, String> update, FailureCallback callback) {
                throw new  IllegalStateException("test");
            }
        };

        try {
            rmap.asyncPut(1, "one").get();
            fail("Exception expected.");
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertSame(ReplicaMapException.class, cause.getClass());

            cause = cause.getCause();
            assertSame(IllegalStateException.class, cause.getClass());
            assertEquals("test", cause.getMessage());
        }

        assertEquals(10, s.availablePermits());
    }

    @Test
    void testSimpleInterrupt() throws InterruptedException {
        Semaphore s = new Semaphore(10);
        TestReplicaMapBase<Integer, String> rmap = new TestReplicaMapBase<Integer, String>('x', new HashMap<>(), s) {
            @Override
            protected void doSendUpdate(TestReplicaMapUpdate<Integer, String> update, FailureCallback callback) {
                // no-op
            }
        };

        Thread.currentThread().interrupt();

        try {
            rmap.asyncPut(1, "one").get();
            fail("Exception expected.");
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertSame(ReplicaMapException.class, cause.getClass());

            cause = cause.getCause();
            assertSame(InterruptedException.class, cause.getClass());
        }

        assertTrue(Thread.interrupted());
        assertEquals(10, s.availablePermits());
    }

    @Test
    void testSimpleCancel() throws InterruptedException, ExecutionException {
        Semaphore s = new Semaphore(10);
        TestReplicaMapBase<Integer, String> rmap = new TestReplicaMapBase<Integer, String>('x', new HashMap<>(), s) {
            @Override
            protected void beforeTryRun(AsyncOp<?,Integer,String> op) {
                op.cancel(false);
            }

            @Override
            protected void doSendUpdate(TestReplicaMapUpdate<Integer, String> update, FailureCallback callback) {
                // no-op
            }
        };

        try {
            rmap.asyncPut(1, "one").get();
            fail("Exception expected.");
        }
        catch (CancellationException e) {
            // no-op
        }

        assertFalse(Thread.interrupted());
        assertEquals(10, s.availablePermits());
    }

    @Test
    void testForwardCompatibility() {
        TestReplicaMapBase<Integer, String> rmap = new TestReplicaMapBase<Integer, String>('x', new HashMap<>(),
            new Semaphore(10), true, 1, TimeUnit.MILLISECONDS) {
            @Override
            protected void doSendUpdate(TestReplicaMapUpdate<Integer, String> update, FailureCallback callback) {
                throw new  IllegalStateException();
            }
        };

        rmap.onReceiveUpdate(false, 100500, (byte)'Z', null, null, null, null, null);
        rmap.onReceiveUpdate(false, 100500, (byte)'Z', 1, null, null, null, null);
    }

    @Test
    void testListener() {
        TestReplicaMapBase<Integer,String> rmap = new TestReplicaMapBase<Integer, String>('x', new HashMap<>(), new Semaphore(10)) {
            @Override
            protected void doSendUpdate(TestReplicaMapUpdate<Integer, String> up, FailureCallback callback) {
                assertEquals('x', up.srcId);
                onReceiveUpdate(true, up.opId, up.updateType, up.key, up.exp, up.upd, up.function, null);
            }
        };

        assertNull(rmap.getListener());

        AtomicInteger cnt = new AtomicInteger();

        ReplicaMapListener<Integer, String> lsnr = (map, myUp, key, oldVal, newVal) -> {
            assertSame(rmap, map);
            assertTrue(myUp);
            assertEquals(1, key);
            assertNull(oldVal);
            assertEquals("1", newVal);
            cnt.incrementAndGet();
        };
        assertTrue(rmap.casListener(null, lsnr));
        assertSame(lsnr, rmap.getListener());
        assertNull(rmap.put(1, "1"));
        assertEquals(1, cnt.get());

        assertFalse(rmap.casListener(null, (map, myUp, key, oldVal, newVal) -> {}));
        assertTrue(rmap.casListener(lsnr, lsnr = (map, myUp, key, oldVal, newVal) -> {
            assertSame(rmap, map);
            assertTrue(myUp);
            assertEquals(1, key);
            assertEquals("1", oldVal);
            assertEquals("one", newVal);
            cnt.incrementAndGet();
        }));
        assertEquals("1", rmap.replace(1, "one"));
        assertSame(lsnr, rmap.getListener());
        assertEquals(2, cnt.get());

        rmap.setListener(lsnr = (map, myUp, key, oldVal, newVal) -> {
            assertSame(rmap, map);
            assertTrue(myUp);
            assertEquals(1, key);
            assertEquals("one", oldVal);
            assertNull(newVal);
            cnt.incrementAndGet();
        });
        assertTrue(rmap.remove(1, "one"));
        assertSame(lsnr, rmap.getListener());
        assertEquals(3, cnt.get());

        assertTrue(rmap.casListener(lsnr, null));
        assertNull(rmap.getListener());

        rmap.setListener((map, myUp, key, oldVal, newVal) -> {
            assertFalse(myUp);
            assertEquals(5, key);
            assertNull(oldVal);
            assertEquals("5", newVal);
            cnt.incrementAndGet();
        });
        rmap.onReceiveUpdate(false, 7, ReplicaMapBase.OP_PUT, 5, null, "5", null, null);
        assertEquals(4, cnt.get());

        rmap.setListener((map, myUp, key, oldVal, newVal) -> {
            cnt.incrementAndGet();
            throw new IllegalStateException("test");
        });
        rmap.put(7, "7");
        assertEquals(5, cnt.get());
        assertEquals("7", rmap.get(7));
        assertTrue(rmap.remove(7, "7"));
        assertNull(rmap.get(7));
    }

    private static <Z> CompletableFuture<Z> assertDone(CompletableFuture<Z> f) {
        assertTrue(f.isDone());
        return f;
    }
}
