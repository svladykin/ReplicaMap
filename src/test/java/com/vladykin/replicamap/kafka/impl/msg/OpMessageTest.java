package com.vladykin.replicamap.kafka.impl.msg;

import com.vladykin.replicamap.kafka.compute.ComputeDeserializer;
import com.vladykin.replicamap.kafka.compute.ComputeSerializer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
    void testMapUpdateMessage() {
        TestStringSerializer tvSer = new TestStringSerializer();
        TestStringDeserializer tvDes = new TestStringDeserializer();

        TestFuncSerializer funSer = new TestFuncSerializer();
        TestFuncDeserializer funDes = new TestFuncDeserializer();

        OpMessageSerializer<String> ser = new OpMessageSerializer<>(tvSer, funSer);
        OpMessageDeserializer<String> des = new OpMessageDeserializer<>(tvDes, funDes);

        ser.configure(null, false);
        des.configure(null, false);

        long clientId = 1;

        String v1 = "abcxyz";
        String v2 = "qwerty";
        BiFunction<?,?,?> function = new TestFunc(7);

        OpMessage msg = new MapUpdateMessage((byte)1, clientId, 1, v1, v2, function);
        byte[] msgBytes = ser.serialize(null, msg);
        assertEquals(1 + 1 + 1 + 1 + 6 + 1 + 6 + 1 + 1, msgBytes.length);
        assertEqualsFull(msg, des.deserialize(null, msgBytes));

        msg = new MapUpdateMessage((byte)1, clientId, 1, null, v2, function);
        msgBytes = ser.serialize(null, msg);
        assertEquals(1 + 1 + 1 + 1 + 0 + 1 + 6 + 1 + 1, msgBytes.length);
        assertEqualsFull(msg, des.deserialize(null, msgBytes));

        msg = new MapUpdateMessage((byte)1, clientId, 1, v1, null, null);
        msgBytes = ser.serialize(null, msg);
        assertEquals(1 + 1 + 1 + 1 + 6 + 1 + 0 + 1 + 0, msgBytes.length);
        assertEqualsFull(msg, des.deserialize(null, msgBytes));

        // compatibility
        msgBytes = Arrays.copyOf(msgBytes, msgBytes.length - 1);
        assertEqualsFull(msg, des.deserialize(null, msgBytes));

        ser.close();
        des.close();

        assertTrue(tvSer.closed);
        assertTrue(tvDes.closed);
        assertTrue(funSer.closed);
        assertTrue(funDes.closed);
    }

    static void assertEqualsFull(OpMessage m1, OpMessage m2) {
        assertEquals(m1, m2);

        if (m1 != null) {
            assertEquals(m1.hashCode(), m2.hashCode());
            assertEquals(m1.toString(), m2.toString());
        }
    }

    @Test
    void testFlushRequest() {
        OpMessageSerializer<Void> ser = new OpMessageSerializer<>((topic, msg) -> null, null);
        OpMessageDeserializer<Void> des = new OpMessageDeserializer<>((topic, msgBytes) -> null, null);

        long clientId = 5;
        long flushOffsetOps = 7;
        long cleanOffsetOps = 11;

        FlushRequest msg = new FlushRequest(clientId, flushOffsetOps, cleanOffsetOps);

        byte[] msgBytes = ser.serialize(null, msg);
        assertEquals(1 + 1 + 1 + 1 + 1, msgBytes.length);

        FlushRequest msgx = (FlushRequest)des.deserialize(null, msgBytes);
        assertEqualsFull(msg, msgx);

        assertEquals(OP_FLUSH_REQUEST, msgx.getOpType());
        assertEquals(clientId, msgx.getClientId());
        assertEquals(flushOffsetOps, msgx.getFlushOffsetOps());
        assertEquals(cleanOffsetOps, msgx.getCleanOffsetOps());
    }

    @Test
    void testFlushNotification() {
        OpMessageSerializer<Void> ser = new OpMessageSerializer<>((topic, msg) -> null, null);
        OpMessageDeserializer<Void> des = new OpMessageDeserializer<>((topic, msgBytes) -> null, null);

        long clientId = 5;
        long flushOffsetOps = 7;
        long flushOffsetData = 9;

        FlushNotification msg = new FlushNotification(clientId, flushOffsetData, flushOffsetOps);

        byte[] msgBytes = ser.serialize(null, msg);
        assertEquals(1 + 1 + 1 + 1 + 1, msgBytes.length);

        FlushNotification msgx = (FlushNotification)des.deserialize(null, msgBytes);
        assertEqualsFull(msg, msgx);

        assertEquals(OP_FLUSH_NOTIFICATION, msgx.getOpType());
        assertEquals(clientId, msgx.getClientId());
        assertEquals(flushOffsetOps, msgx.getFlushOffsetOps());
        assertEquals(flushOffsetData, msgx.getFlushOffsetData());
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

    static class TestFuncSerializer extends ConfigurableCloseable implements ComputeSerializer {
        @Override
        public byte[] serialize(String topic, BiFunction<?,?,?> data) {
            assertTrue(configured);
            assertFalse(closed);
            return new byte[]{(byte)((TestFunc)data).x};
        }

        @Override
        public boolean canSerialize(BiFunction<?,?,?> function) {
            return function instanceof TestFunc;
        }
    }

    static class TestFuncDeserializer extends ConfigurableCloseable implements ComputeDeserializer {
        public BiFunction<?,?,?> deserialize(String topic, byte[] data) {
            assertTrue(configured);
            assertFalse(closed);
            return new TestFunc(data[0]);
        }
    }

    static class TestFunc implements BiFunction<Integer,Integer,Integer> {
        int x;

        TestFunc(int x) {
            this.x = x;
        }

        @Override
        public Integer apply(Integer k, Integer v) {
            return v + x;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestFunc testFunc = (TestFunc)o;

            return x == testFunc.x;
        }

        @Override
        public int hashCode() {
            return x;
        }
    }
}
