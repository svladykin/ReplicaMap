package com.vladykin.replicamap.kafka.impl.msg;

import com.vladykin.replicamap.kafka.compute.ComputeSerializer;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ByteUtils;

import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_REQUEST;

/**
 * Operation message serializer.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class OpMessageSerializer<V> implements Serializer<OpMessage> {
    public static final int NULL_ARRAY_LENGTH = -1;

    protected final Serializer<V> valSer;
    protected final ComputeSerializer funSer;

    public OpMessageSerializer(Serializer<V> valSer, ComputeSerializer funSer) {
        this.valSer = Utils.requireNonNull(valSer, "valSer");
        this.funSer = funSer;
    }

    @Override
    public void configure(Map<String,?> configs, boolean isKey) {
        valSer.configure(configs, isKey);

        if (funSer != null)
            funSer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, OpMessage opMsg) {
        switch (opMsg.getOpType()) {
            case OP_FLUSH_REQUEST:
                return serializeFlushRequest((FlushRequest)opMsg);

            case OP_FLUSH_NOTIFICATION:
                return serializeFlushNotification((FlushNotification)opMsg);
        }

        return serializeMapUpdateMessage((MapUpdate)opMsg, topic);
    }

    @SuppressWarnings("unchecked")
    protected byte[] serializeMapUpdateMessage(MapUpdate opMsg, String topic) {
        V expVal = (V)opMsg.getExpectedValue();
        byte[] exp = expVal == null ? null : valSer.serialize(topic, expVal);

        V updVal = (V)opMsg.getUpdatedValue();
        byte[] upd = updVal == null ? null : valSer.serialize(topic, updVal);

        BiFunction<?,?,?> funVal = opMsg.getFunction();
        byte[] fun = funVal == null ? null : funSer.serialize(topic, funVal);

        int opTypeSize = 1;
        int clientIdSize = ByteUtils.sizeOfVarlong(opMsg.getClientId());
        int opIdSize = ByteUtils.sizeOfVarlong(opMsg.getOpId());
        int expLen = arrayLength(exp);
        int updLen = arrayLength(upd);
        int funLen = arrayLength(fun);
        int expLenSize = ByteUtils.sizeOfVarint(expLen);
        int updLenSize = ByteUtils.sizeOfVarint(updLen);
        int funLenSize = ByteUtils.sizeOfVarint(funLen);

        int resultLen = opTypeSize + clientIdSize + opIdSize + expLenSize + updLenSize + funLenSize;

        if (expLen > 0)
            resultLen += expLen;
        if (updLen > 0)
            resultLen += updLen;
        if (funLen > 0)
            resultLen += funLen;

        byte[] result = new byte[resultLen];
        ByteBuffer buf = ByteBuffer.wrap(result);

        buf.put(opMsg.getOpType());
        ByteUtils.writeVarlong(opMsg.getClientId(), buf);
        ByteUtils.writeVarlong(opMsg.getOpId(), buf);
        writeByteArray(buf, exp);
        writeByteArray(buf, upd);
        writeByteArray(buf, fun);

        assert buf.remaining() == 0;

        return result;
    }

    protected byte[] serializeFlushRequest(FlushRequest flushMsg) {
        int opTypeSize = 1;
        int clientIdSize = ByteUtils.sizeOfVarlong(flushMsg.getClientId());
        int flushOffsetOpsSize = ByteUtils.sizeOfVarlong(flushMsg.getFlushOffsetOps());
        int cleanOffsetOpsSize = ByteUtils.sizeOfVarlong(flushMsg.getCleanOffsetOps());

        byte[] result = new byte[opTypeSize + clientIdSize + 1 + flushOffsetOpsSize + cleanOffsetOpsSize];
        ByteBuffer buf = ByteBuffer.wrap(result);

        buf.put(flushMsg.getOpType());
        ByteUtils.writeVarlong(flushMsg.getClientId(), buf);
        ByteUtils.writeVarlong(0L, buf); // Backward compatibility.
        ByteUtils.writeVarlong(flushMsg.getFlushOffsetOps(), buf);
        ByteUtils.writeVarlong(flushMsg.getCleanOffsetOps(), buf);

        assert buf.remaining() == 0;

        return result;
    }

    protected byte[] serializeFlushNotification(FlushNotification flushMsg) {
        int opTypeSize = 1;
        int clientIdSize = ByteUtils.sizeOfVarlong(flushMsg.getClientId());
        int flushOffsetDataSize = ByteUtils.sizeOfVarlong(flushMsg.getFlushOffsetData());
        int flushOffsetOpsSize = ByteUtils.sizeOfVarlong(flushMsg.getFlushOffsetOps());

        byte[] result = new byte[opTypeSize + clientIdSize + flushOffsetDataSize + flushOffsetOpsSize + 1];
        ByteBuffer buf = ByteBuffer.wrap(result);

        buf.put(flushMsg.getOpType());
        ByteUtils.writeVarlong(flushMsg.getClientId(), buf);
        ByteUtils.writeVarlong(flushMsg.getFlushOffsetData(), buf);
        ByteUtils.writeVarlong(flushMsg.getFlushOffsetOps(), buf);
        ByteUtils.writeVarlong(0L, buf); // Backward compatibility.

        assert buf.remaining() == 0;

        return result;
    }

    protected int arrayLength(byte[] arr) {
        return arr == null ? NULL_ARRAY_LENGTH : arr.length;
    }

    protected void writeByteArray(ByteBuffer buf, byte[] arr) {
        if (arr == null) {
            ByteUtils.writeVarint(NULL_ARRAY_LENGTH, buf);
        } else {
            ByteUtils.writeVarint(arr.length, buf);
            buf.put(arr);
        }
    }

    @Override
    public void close() {
        Utils.close(valSer);
        Utils.close(funSer);
    }
}