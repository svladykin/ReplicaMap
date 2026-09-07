package com.vladykin.replicamap.kafka.impl.util;

import com.vladykin.replicamap.ReplicaMapException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static com.vladykin.replicamap.kafka.impl.util.Utils.assignPartitionsRoundRobin;
import static com.vladykin.replicamap.kafka.impl.util.Utils.deserializeShortArray;
import static com.vladykin.replicamap.kafka.impl.util.Utils.serializeShortArray;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UtilsTest {
    @Test
    void testCheckPositive() {
        assertThrows(ReplicaMapException.class, () -> Utils.checkPositive(0, "x"));
        assertThrows(ReplicaMapException.class, () -> Utils.checkPositive(-1, "x"));
        Utils.checkPositive(1, "x");
    }

    /**
     * A worker blocked on {@code opsSteadyFut.get()} receives an interruption wrapped into
     * {@link ExecutionException} / {@link CompletionException}. Missing those wrappers makes a clean
     * shutdown look like an unrecoverable failure.
     */
    @Test
    void testIsInterrupted() {
        assertFalse(Utils.isInterrupted(null));

        // Directly interrupted.
        assertTrue(Utils.isInterrupted(new InterruptedException()));
        assertTrue(Utils.isInterrupted(new InterruptException("x")));
        assertTrue(Utils.isInterrupted(new WakeupException()));

        // Through the known wrappers, in any combination.
        assertTrue(Utils.isInterrupted(new ReplicaMapException("x", new InterruptedException())));
        assertTrue(Utils.isInterrupted(new ExecutionException(new InterruptedException())));
        assertTrue(Utils.isInterrupted(new CompletionException(new WakeupException())));
        assertTrue(Utils.isInterrupted(
            new CompletionException(new ReplicaMapException("x", new WakeupException()))));

        // The real chain a flush worker sees when the ops workers are interrupted while starting.
        assertTrue(Utils.isInterrupted(new ExecutionException(
            new ReplicaMapException("failed to start",
                new CompletionException(
                    new ReplicaMapException("interrupted", new InterruptedException()))))));

        // Real failures, including the ones wrapped the same way.
        assertFalse(Utils.isInterrupted(new IllegalStateException("boom")));
        assertFalse(Utils.isInterrupted(new ExecutionException(new IllegalStateException("boom"))));
        assertFalse(Utils.isInterrupted(new ReplicaMapException("x", new IllegalStateException("boom"))));

        // An unknown wrapper must not be unwrapped: a genuine failure that merely carries an
        // interruption in its cause chain is still a failure.
        assertFalse(Utils.isInterrupted(new IllegalStateException("boom", new InterruptedException())));

        // A cyclic cause chain must terminate instead of spinning (Java forbids direct
        // self-causation, but a->b->a is constructible).
        ReplicaMapException a = new ReplicaMapException("a");
        ReplicaMapException b = new ReplicaMapException("b", a);
        a.initCause(b);
        assertFalse(Utils.isInterrupted(a));
    }

    @Test
    void testToNanosSaturated() {
        assertEquals(0L, Utils.toNanosSaturated(Duration.ZERO));
        assertEquals(1_000_000L, Utils.toNanosSaturated(Duration.ofMillis(1)));
        assertEquals(-1_000_000L, Utils.toNanosSaturated(Duration.ofMillis(-1)));

        // Would throw ArithmeticException from Duration.toNanos().
        assertEquals(Long.MAX_VALUE, Utils.toNanosSaturated(Duration.ofMillis(Long.MAX_VALUE)));
        assertEquals(Long.MIN_VALUE, Utils.toNanosSaturated(Duration.ofMillis(Long.MIN_VALUE)));
    }

    @Test
    void testAssignPartitionsRoundRobin() {
        assertEquals(new HashSet<>(asList(0, 3, 6)),
            assignPartitionsRoundRobin(0, 3, 8, null));
        assertEquals(new HashSet<>(asList(1, 4, 7)),
            assignPartitionsRoundRobin(1, 3, 8, null));
        assertEquals(new HashSet<>(asList(2, 5)),
            assignPartitionsRoundRobin(2, 3, 8, null));

        assertEquals(new HashSet<>(asList(0, 4, 8)),
            assignPartitionsRoundRobin(0, 4, 12, null));
        assertEquals(new HashSet<>(asList(1, 5, 9)),
            assignPartitionsRoundRobin(1, 4, 12, null));
        assertEquals(new HashSet<>(asList(2, 6, 10)),
            assignPartitionsRoundRobin(2, 4, 12, null));
        assertEquals(new HashSet<>(asList(3, 7, 11)),
            assignPartitionsRoundRobin(3, 4, 12, null));

        assertEquals(new HashSet<>(asList(1, 6)),
            assignPartitionsRoundRobin(0, 3, 8, new short[]{1,2,5,6}));
        assertEquals(new HashSet<>(asList(6)),
            assignPartitionsRoundRobin(1, 3, 8, new short[]{2,6,7}));
        assertEquals(new HashSet<>(asList(2, 6)),
            assignPartitionsRoundRobin(2, 3, 8, new short[]{0,1,2,3,5,6,7}));
    }

    @Test
    void testParseIntSet() {
        assertNull(Utils.parseIntSet(null));
        assertEquals(new HashSet<>(), Utils.parseIntSet(emptyList()));
        assertEquals(new HashSet<>(Arrays.asList(100)), Utils.parseIntSet(Arrays.asList("100")));
        assertEquals(new HashSet<>(Arrays.asList(1,5)), Utils.parseIntSet(Arrays.asList("1","5")));
        assertEquals(new HashSet<>(Arrays.asList(0,555,1000)), Utils.parseIntSet(Arrays.asList("1000","555","0")));
        assertEquals(new HashSet<>(Arrays.asList(-1000,0,555)), Utils.parseIntSet(Arrays.asList("-1000","555","0")));

        assertThrows(NumberFormatException.class, () -> Utils.parseIntSet(Arrays.asList("1", "bla")));
    }

    @Test
    void testContains() {
        assertFalse(Utils.contains(new short[] {1, 2, 3}, (short)0));
        assertTrue(Utils.contains(new short[] {1, 2, 3}, (short)1));
        assertTrue(Utils.contains(new short[] {1, 2, 3}, (short)2));
        assertTrue(Utils.contains(new short[] {1, 2, 3}, (short)3));
        assertFalse(Utils.contains(new short[] {1, 2, 3}, (short)4));

        short[] arr = new short[100];
        for (int i = 0; i < arr.length; i++)
            arr[i] = (short)(i * 2);

//        System.out.println(Arrays.toString(arr));

        for (int i = 0; i < 101; i++) {
//            System.out.println(i);
            assertEquals((i & 1) == 0, Utils.contains(arr, (short)i));
        }
    }

    @Test
    void testSerializeShortArray() {
        assertNull(serializeShortArray(null));
        assertNull(deserializeShortArray(null));

        ByteBuffer buf = ByteBuffer.allocate(10);
        buf.position(10);
        assertNull(deserializeShortArray(buf));

        checkSerializeShortArray(new short[]{}, 1);
        checkSerializeShortArray(new short[]{1}, 2);
        checkSerializeShortArray(new short[]{1,2}, 3);
        checkSerializeShortArray(new short[]{63}, 2);
        checkSerializeShortArray(new short[]{64}, 3);

        checkSerializeShortArray(new short[63], 64);
        checkSerializeShortArray(new short[64], 66);
    }

    void checkSerializeShortArray(short[] arr, int expLen) {
        byte[] bytes = serializeShortArray(arr);
        assertEquals(expLen, bytes.length);
        assertArrayEquals(arr, deserializeShortArray(ByteBuffer.wrap(bytes)));
    }
}
