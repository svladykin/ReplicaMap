package com.vladykin.replicamap.kafka.impl.msg;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import static com.vladykin.replicamap.base.ReplicaMapBase.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.base.ReplicaMapBase.OP_FLUSH_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpMessageTest {
    @SuppressWarnings("PointlessArithmeticExpression")
    @Test
    void testOperationMessageSerdeProtobuf() {
        TestStringSerializer tvSer = new TestStringSerializer();
        TestStringDeserializer tvDes = new TestStringDeserializer();

        OpMessageSerializer<String> ser = new OpMessageSerializer<>(tvSer);
        OpMessageDeserializer<String> des = new OpMessageDeserializer<>(tvDes);

        ser.configure(null, false);
        des.configure(null, false);

        long clientId = 1;

        String v1 = "abcxyz";
        String v2 = "qwerty";
        BiFunction<?,?,?> function = null;

        OpMessage msg = new OpMessage((byte)1, clientId, 1, v1, v2, function);
        byte[] msgBytes = ser.serialize(null, msg);
        assertEquals(1 + 1 + 1 + 1 + 6 + 1 + 6, msgBytes.length);
        assertEquals(msg, des.deserialize(null, msgBytes));

        msg = new OpMessage((byte)1, clientId, 1, null, v2, function);
        msgBytes = ser.serialize(null, msg);
        assertEquals(1 + 1 + 1 + 1 + 0 + 1 + 6, msgBytes.length);
        assertEquals(msg, des.deserialize(null, msgBytes));

        msg = new OpMessage((byte)1, clientId, 1, v1, null, function);
        msgBytes = ser.serialize(null, msg);
        assertEquals(1 + 1 + 1 + 1 + 6 + 1 + 0, msgBytes.length);
        assertEquals(msg, des.deserialize(null, msgBytes));

        ser.close();
        des.close();

        assertTrue(tvSer.closed);
        assertTrue(tvDes.closed);
    }

    @Test
    void testFlushRequest() {
        OpMessageSerializer<Void> ser = new OpMessageSerializer<>((topic, msg) -> null);
        OpMessageDeserializer<Void> des = new OpMessageDeserializer<>((topic, msgBytes) -> null);

        long clientId = 5;
        long flushOffsetOps = 7;
        long flushOffsetData = 4;
        long cleanOffsetOps = 11;

        OpMessage msg = new OpMessage(OP_FLUSH_REQUEST, clientId, flushOffsetData, flushOffsetOps, cleanOffsetOps);

        byte[] msgBytes = ser.serialize(null, msg);
        assertEquals(1 + 1 + 1 + 1 + 1, msgBytes.length);

        OpMessage msgx = des.deserialize(null, msgBytes);
        assertEquals(msg, msgx);

        assertEquals(OP_FLUSH_REQUEST, msgx.getOpType());
        assertEquals(clientId, msgx.getClientId());
        assertEquals(flushOffsetOps, msgx.getFlushOffsetOps());
        assertEquals(flushOffsetData, msgx.getFlushOffsetData());
        assertEquals(cleanOffsetOps, msgx.getCleanOffsetOps());
    }

    @Test
    void testFlushNotification() {
        OpMessageSerializer<Void> ser = new OpMessageSerializer<>((topic, msg) -> null);
        OpMessageDeserializer<Void> des = new OpMessageDeserializer<>((topic, msgBytes) -> null);

        long clientId = 5;
        long flushOffsetOps = 7;
        long flushOffsetData = 9;
        long lastCleanOffsetOps = 8;

        OpMessage msg = new OpMessage(OP_FLUSH_NOTIFICATION, clientId, flushOffsetData, flushOffsetOps, lastCleanOffsetOps);

        byte[] msgBytes = ser.serialize(null, msg);
        assertEquals(1 + 1 + 1 + 1 + 1, msgBytes.length);

        OpMessage msgx = des.deserialize(null, msgBytes);
        assertEquals(msg, msgx);

        assertEquals(OP_FLUSH_NOTIFICATION, msgx.getOpType());
        assertEquals(clientId, msgx.getClientId());
        assertEquals(flushOffsetOps, msgx.getFlushOffsetOps());
        assertEquals(flushOffsetData, msgx.getFlushOffsetData());
        assertEquals(lastCleanOffsetOps, msgx.getCleanOffsetOps());
    }

    static class ConfigurableCloseable {
        boolean configured;
        boolean closed;

        @SuppressWarnings("unused")
        public void configure(Map<String,?> configs, boolean isKey) {
            configured = true;
        }

        public void close() {
            closed = true;
        }
    }

    static class TestStringSerializer extends ConfigurableCloseable implements Serializer<String> {
        @Override
        public byte[] serialize(String topic, String data) {
            assertTrue(configured);
            assertFalse(closed);
            return data.getBytes(StandardCharsets.UTF_8);
        }
    }

    static class TestStringDeserializer extends ConfigurableCloseable implements Deserializer<String> {
        public String deserialize(String topic, byte[] data) {
            assertTrue(configured);
            assertFalse(closed);
            return new String(data, StandardCharsets.UTF_8);
        }
    }
}
