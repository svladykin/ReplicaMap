package com.vladykin.replicamap.kafka;

import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.BOOTSTRAP_SERVERS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.FLUSH_MAX_UNFLUSHED_MS;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.NO_LIMIT;
import static com.vladykin.replicamap.kafka.KReplicaMapManagerConfig.STOP_TIMEOUT_MS;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Both {@code flush.max.unflushed.ms} and {@code stop.timeout.ms} share one rule: positive, or
 * {@link KReplicaMapManagerConfig#NO_LIMIT}. Configuration is validated before the manager touches
 * Kafka, so these need no broker.
 */
class KReplicaMapManagerConfigTest {

    Map<String,Object> getConfig(String name, Object value) {
        HashMap<String,Object> cfg = new HashMap<>();
        cfg.put(BOOTSTRAP_SERVERS, singletonList("localhost:9092")); // Never connected to.
        cfg.put(name, value);
        return cfg;
    }

    ReplicaMapException assertRejected(String name, long value) {
        return assertThrows(ReplicaMapException.class, () -> new KReplicaMapManager(getConfig(name, value)));
    }

    /**
     * {@code 0} reads as "immediately" on a {@code *.ms} setting, so it must fail loudly and point at
     * {@code -1} instead of silently meaning the opposite ("never").
     */
    @Test
    void testZeroIsRejected() {
        for (String name : new String[] {FLUSH_MAX_UNFLUSHED_MS, STOP_TIMEOUT_MS}) {
            ReplicaMapException e = assertRejected(name, 0);

            assertTrue(e.getMessage().contains(name), "Must name the property: " + e.getMessage());
            assertTrue(e.getMessage().contains(String.valueOf(NO_LIMIT)),
                "Must point at the no-limit value: " + e.getMessage());
        }
    }

    /** Only the {@link KReplicaMapManagerConfig#NO_LIMIT} sentinel is a legal non-positive value. */
    @Test
    void testOtherNegativesAreRejected() {
        for (String name : new String[] {FLUSH_MAX_UNFLUSHED_MS, STOP_TIMEOUT_MS}) {
            ReplicaMapException e = assertRejected(name, -2);

            assertTrue(e.getMessage().contains(name), "Must name the property: " + e.getMessage());
        }
    }

    /**
     * {@code -1} must pass validation, which is what tells it apart from the rejected values above.
     *
     * <p>The manager is made to fail right after the validation, on the first client it creates:
     * otherwise it would go on to fetch the partitions and block for {@code max.block.ms} against the
     * absent broker, and that is not tunable from here — the manager forwards only
     * {@code bootstrap.servers} to its Kafka clients.
     */
    @Test
    void testNoLimitIsAccepted() {
        IllegalStateException pastValidation = new IllegalStateException("past config validation");

        for (String name : new String[] {FLUSH_MAX_UNFLUSHED_MS, STOP_TIMEOUT_MS}) {
            ReplicaMapException e = assertThrows(ReplicaMapException.class,
                () -> new KReplicaMapManager(getConfig(name, NO_LIMIT)) {
                    @Override
                    protected Producer<Object,OpMessage> newKafkaProducerOps() {
                        throw pastValidation;
                    }
                });

            assertSame(pastValidation, e.getCause(),
                "Expected to get past the config validation for " + name + ", got: " + e);
        }
    }
}
