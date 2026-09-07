package com.vladykin.replicamap.kafka.impl.util;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;

/**
 * Utility methods.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public final class Utils {
//    public static final Logger trace = LoggerFactory.getLogger(Utils.class.getName() + ".trace");

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static final Duration MIN_POLL_TIMEOUT = Duration.ofMillis(5);

    public static final int UUID_SIZE_BYTES = 2 * Long.BYTES;
    public static final int NULL_ARRAY_LENGTH = -1;

    /** Max number of causes {@link #isInterrupted(Throwable)} unwraps, so a cyclic chain terminates. */
    private static final int MAX_CAUSE_CHAIN_DEPTH = 64;

    public static <K,V> ConsumerRecords<K,V> poll(Consumer<K,V> c, Duration timeout) {
        if (timeout.compareTo(MIN_POLL_TIMEOUT) < 0) // "poll" may often produce empty results and break tests for small timeouts
            timeout = MIN_POLL_TIMEOUT;

        return c.poll(timeout);
    }

    public static <X> X ifNull(X x, X dflt) {
        return x != null ? x : dflt;
    }

    public static <X> X ifNull(X x, Supplier<X> dflt) {
        return x != null ? x : dflt.get();
    }

    public static void close(Iterable<? extends AutoCloseable> cs) {
        if (cs == null)
            return;

        for (AutoCloseable c : cs)
            close(c);
    }

    /**
     * Best effort release of an internal resource: a failure is logged and swallowed.
     * Not for lifecycle objects whose {@code close()} blocks, like {@link ReplicaMapManager} —
     * for those swallowing the failure would hide that the resources are still held.
     *
     * @param c Resource to close.
     */
    public static void close(AutoCloseable c) {
        if (c != null) {
            try {
                c.close();
            }
            catch (Exception e) {
                if (isInterrupted(e))
                    Thread.currentThread().interrupt();
                else
                    log.error("Failed to close {}", c, e);
            }
        }
    }

    /**
     * @param e Exception to inspect.
     * @return {@code true} if the exception, or one of the exceptions it wraps, is an interruption.
     */
    public static boolean isInterrupted(Throwable e) {
        // Bounded: real chains are a few levels deep, and a cyclic one must not spin forever.
        int depth = MAX_CAUSE_CHAIN_DEPTH;

        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof InterruptedException ||
                t instanceof InterruptException ||
                t instanceof WakeupException)
                return true;

            // Unwrap only the known wrappers: an arbitrary exception that merely happens to carry
            // an interruption somewhere down its cause chain is a real failure, not an interruption.
            if (!(t instanceof ReplicaMapException ||
                  t instanceof ExecutionException ||
                  t instanceof CompletionException))
                return false;

            if (--depth == 0)
                return false;
        }

        return false;
    }

    public static String getMessage(Exception e) {
        String name = e.getClass().getSimpleName();
        String msg = e.getMessage();
        return msg != null ? name + ": " + msg : name;
    }

    public static void maybeClose(Object c) {
        if (c instanceof AutoCloseable)
            close((AutoCloseable)c);
    }

    public static CompletableFuture<Void> allOf(Collection<? extends CompletableFuture<?>> futs) {
        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
    }

    public static <T> T requireNonNull(T obj, String varName) {
        if (obj == null)
            throw new NullPointerException(varName + " is null");

        return obj;
    }

    public static void check(boolean check, Supplier<String> msg) {
        if (!check)
            throw new ReplicaMapException(msg.get());
    }

    public static void checkPositive(long x, String varName) {
        if (x <= 0)
            throw new ReplicaMapException(varName + " must be positive");
    }

    public static void checkPositive(Duration x, String varName) {
        if (x.isNegative() || x.isZero())
            throw new ReplicaMapException(varName + " must be positive");
    }

    /**
     * {@link Duration#toNanos()} that saturates instead of overflowing, so that a huge configured
     * duration can never fail with an obscure {@link ArithmeticException}.
     *
     * @param x Duration.
     * @return Nanoseconds, or {@link Long#MAX_VALUE} if the duration does not fit.
     */
    public static long toNanosSaturated(Duration x) {
        try {
            return x.toNanos();
        }
        catch (ArithmeticException ignored) {
            return x.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
    }

    public static int cpus() {
        return Runtime.getRuntime().availableProcessors();
    }

    public static boolean contains(short[] sortedArr, short x) {
        return indexOf(sortedArr, x) >= 0;
    }

    public static int indexOf(short[] sortedArr, short x) {
        if (sortedArr.length <= 32) {
            for (int i = 0; i < sortedArr.length; i++) {
                short y = sortedArr[i];

                if (x == y)
                    return i;

                if (x < y)
                    return -1;
            }

            return -1;
        }

        return Arrays.binarySearch(sortedArr, x);
    }

    public static Set<Integer> assignPartitionsRoundRobin(int workerId, int allWorkers, int allParts, short[] allowedParts) {
        if (allowedParts != null)
            allParts = allowedParts.length;

        Set<Integer> assignedParts = new TreeSet<>();

        for (int part = 0; part < allParts; part++) {
            if (part % allWorkers == workerId)
                assignedParts.add(allowedParts == null ? part : allowedParts[part]);
        }

        return assignedParts;
    }

    public static Set<Integer> parseIntSet(List<String> list) {
        if (list == null)
            return null;

        Set<Integer> set = new HashSet<>();

        for (String p : list)
            set.add(Integer.parseInt(p));

        return set;
    }

    public static void wakeup(Consumer<?,?> c) {
        if (c != null) {
            try {
                c.wakeup();
            }
            catch (Exception e) {
                log.error("Failed to wakeup consumer.", e);
            }
        }
    }

    public static void wakeup(Supplier<Consumer<?,?>> s) {
        if (s != null) {
            Consumer<?,?> c;
            try {
                c = s.get();
            }
            catch (Exception e) {
                if (!isInterrupted(e))
                    log.error("Failed to get consumer for wakeup.", e);

                return;
            }
            wakeup(c);
        }
    }

    public static List<TopicPartition> partitions(Consumer<?,?> consumer, String topic) {
        List<PartitionInfo> parts = consumer.partitionsFor(topic);

        if (parts == null || parts.isEmpty())
            throw new ReplicaMapException("Failed to fetch partitions for topic: " + topic);

        return parts.stream()
            .map(p -> new TopicPartition(topic, p.partition()))
            .collect(Collectors.toList());
    }

    public static long endOffset(Consumer<?,?> consumer, TopicPartition part) {
        Map<TopicPartition,Long> endOffsets = consumer.endOffsets(singleton(part));

        if (endOffsets == null || endOffsets.isEmpty())
            throw new ReplicaMapException("Failed to fetch end offset for partition: " + part);

        return endOffsets.get(part);
    }

    public static long beginningOffset(Consumer<?,?> consumer, TopicPartition part) {
        Map<TopicPartition,Long> beginningOffsets = consumer.beginningOffsets(singleton(part));

        if (beginningOffsets == null || beginningOffsets.isEmpty())
            throw new ReplicaMapException("Failed to fetch beginning offset for partition: " + part);

        return beginningOffsets.get(part);
    }

    public static <T> T getConfiguredInstance(Class<T> clazz, Map<String,?> configs) {
        T instance = org.apache.kafka.common.utils.Utils.newInstance(clazz);

        if (instance instanceof Configurable)
            ((Configurable) instance).configure(configs);

        return instance;
    }

    public static byte[] serializeShortArray(short[] arr) {
        if (arr == null)
            return null;

        int len = ByteUtils.sizeOfVarint(arr.length);
        for (short x : arr)
            len += ByteUtils.sizeOfVarint(x);

        ByteBuffer buf = ByteBuffer.allocate(len);
        ByteUtils.writeVarint(arr.length, buf);

        for (short x : arr)
            ByteUtils.writeVarint(x, buf);

        assert buf.remaining() == 0;
        return buf.array();
    }

    public static int getArrayLength(byte[] arr) {
        return arr == null ? NULL_ARRAY_LENGTH : arr.length;
    }

    public static byte[] readByteArray(ByteBuffer buf) {
        int len = ByteUtils.readVarint(buf);

        if (len == NULL_ARRAY_LENGTH)
            return null;

        byte[] arr = new byte[len];

        if (len != 0)
            buf.get(arr);

        return arr;
    }

    public static void writeByteArray(byte[] arr, ByteBuffer buf) {
        if (arr == null) {
            ByteUtils.writeVarint(NULL_ARRAY_LENGTH, buf);
        } else {
            ByteUtils.writeVarint(arr.length, buf);
            buf.put(arr);
        }
    }

    public static UUID readUuid(ByteBuffer buf) {
        assert buf.order() == ByteOrder.BIG_ENDIAN;

        long msb = buf.getLong();
        long lsb = buf.getLong();
        return new UUID(msb, lsb);
    }

    public static void writeUuid(UUID id, ByteBuffer buf) {
        assert buf.order() == ByteOrder.BIG_ENDIAN;

        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();

        buf.putLong(msb);
        buf.putLong(lsb);
    }

    public static short[] deserializeShortArray(ByteBuffer buf) {
        if (buf == null || buf.remaining() == 0)
            return null;

        int len = ByteUtils.readVarint(buf);
        short[] arr = new short[len];

        for (int i = 0; i < len; i++)
            arr[i] = (short)ByteUtils.readVarint(buf);

        return arr;
    }

    public static byte[] serializeVarlong(long x) {
        byte[] bytes = new byte[ByteUtils.sizeOfVarlong(x)];
        ByteUtils.writeVarlong(x, ByteBuffer.wrap(bytes));
        return bytes;
    }

    public static long deserializeVarlong(byte[] bytes) {
        return ByteUtils.readVarlong(ByteBuffer.wrap(bytes));
    }
}
