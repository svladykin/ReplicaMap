package com.vladykin.replicamap.kafka.impl.util;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.MiniRecord;
import java.net.NetworkInterface;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singleton;

/**
 * Utility methods.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public final class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    private static final Duration[] SECONDS = new Duration[1000];
    private static final Duration[] MILLIS = new Duration[1000];

    public static Duration seconds(int sec) {
        return duration(SECONDS, sec, ChronoUnit.SECONDS);
    }

    public static Duration millis(long ms) {
        return duration(MILLIS, ms, ChronoUnit.MILLIS);
    }

    private static Duration duration(Duration[] cache, long x, ChronoUnit u) {
        if (x >= cache.length)
            return Duration.of(x, u);

        Duration d = cache[(int)x];
        if (d == null)
            cache[(int)x] = d = Duration.of(x, u);
        return d;
    }

    public static <X> X ifNull(X x, X dflt) {
        return x != null ? x : dflt;
    }

    public static <X> X ifNull(X x, Supplier<X> dflt) {
        return x != null ? x : dflt.get();
    }

    public static List<byte[]> getMacAddresses() {
        List<byte[]> macs = new ArrayList<>();
        Enumeration<NetworkInterface> ifaces = null;

        try {
            ifaces = NetworkInterface.getNetworkInterfaces();
        }
        catch (Exception e) {
            log.warn("Failed to get network interfaces.", e);
        }

        if (ifaces != null) {
            while (ifaces.hasMoreElements()) {
                NetworkInterface iface = ifaces.nextElement();
                byte[] macAddr = null;

                try {
                    if (iface != null && !iface.isLoopback() && !iface.isPointToPoint() && !iface.isVirtual())
                        macAddr = iface.getHardwareAddress();
                }
                catch (Exception e) {
                    log.warn("Failed to get MAC address.", e);
                }

                if (macAddr != null && macAddr.length > 0)
                    macs.add(macAddr);
            }
        }

        return macs;
    }

    /**
     * Generates unique 8 bytes node id for a small cluster.
     * Takes 4 bytes from the current time, 1 byte hash from macs and 3 random bytes.
     * This gives very low practical probability of clashes for small clusters
     * even if there are multiple nodes started on the same host:
     *   timestamp part gives ~50 days precision (have to hit the same millisecond for a clash);
     *   mac hash rarely clashes for small clusters (~5% probability of 1 clash for 10 hosts, ~32% for 20 hosts);
     *   for resolving time/mac clashes there are 3 random bytes (useful when running tests with multiple nodes
     *       on a single host).
     *
     * @param currentTimeMillis Current timestamp in milliseconds.
     * @param macs List of mac addresses.
     * @param secureRnd Secure random.
     * @return Unique 8 byte node id.
     */
    public static long generateUniqueNodeId(long currentTimeMillis, List<byte[]> macs, Random secureRnd) {
        Utils.requireNonNull(secureRnd, "secureRnd");

        // Drop higher 32 bits, they change too rare.
        // The remaining lower 32 bits give around 50 days precision.
        long time = currentTimeMillis & 0xFFFFFFFFL;

        long rnd;
        if (macs == null || macs.isEmpty())
            rnd = secureRnd.nextInt(); // take 4 random bytes if we do not have macs
        else {
            long mac = macHash1(macs) & 0xFF;
            rnd = secureRnd.nextInt(0x1000000); // take 3 random bytes
            rnd |= mac << 24; // concatenate mac + rnd into 4 bytes
        }

        return (time << 32) | rnd;
    }

    public static byte rotateRight(byte b, int shift) {
        assert shift >= 0: shift;
        shift = shift & 7;

        int x = b & 0xFF;
        return (byte)((x << (8 - shift)) | (x >>> shift));
    }

    public static byte macHash1(List<byte[]> macs) {
        byte mac = 0;
        int i = 0;
        for (byte[] macAddr : macs) {
            for (byte b : macAddr)
                mac ^= rotateRight(b, i++);
        }
        return mac;
    }

    public static void close(Iterable<? extends AutoCloseable> cs) {
        if (cs == null)
            return;

        for (AutoCloseable c : cs)
            close(c);
    }

    public static void close(AutoCloseable c) {
        if (c != null) {
            try {
                c.close();
            }
            catch (Exception e) {
                if (isInterrupted(e))
                    Thread.currentThread().interrupt();
                else
                    log.error("Failed to close " + c, e);
            }
        }
    }

    public static boolean isInterrupted(Exception e) {
        return e instanceof InterruptedException ||
               e instanceof InterruptException ||
               e instanceof WakeupException;
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

    public static int cpus() {
        return Runtime.getRuntime().availableProcessors();
    }

    public static boolean isOverMaxOffset(ConsumerRecord<?,?> rec, long maxOffset) {
        return rec.offset() > maxOffset;
    }

    public static boolean contains(short[] sortedArr, short x) {
        if (sortedArr.length <= 32) {
            for (int y : sortedArr) {
                if (x == y)
                    return true;

                if (x < y)
                    return false;
            }

            return false;
        }

        return Arrays.binarySearch(sortedArr, x) >= 0;
    }

    public static boolean isOverMaxOffset(MiniRecord rec, long maxOffset) {
        return rec.offset() > maxOffset;
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

    public static Map<TopicPartition,Long> endOffsets(Consumer<?,?> consumer, String topic) {
        return consumer.endOffsets(partitions(consumer, topic));
    }

    public static long endOffset(Consumer<?,?> consumer, TopicPartition part) {
        Map<TopicPartition,Long> endOffsets = consumer.endOffsets(singleton(part));

        if (endOffsets == null || endOffsets.isEmpty())
            throw new ReplicaMapException("Failed to fetch end offset for partition: " + part);

        return endOffsets.get(part);
    }

    public static boolean isEndPosition(Consumer<?,?> consumer, TopicPartition part) {
        return endOffset(consumer, part) == consumer.position(part);
    }

    public static <T> T getConfiguredInstance(Class<T> clazz, Map<String,?> configs) {
        T instance = org.apache.kafka.common.utils.Utils.newInstance(clazz);

        if (instance instanceof Configurable)
            ((Configurable) instance).configure(configs);

        return instance;
    }
}
