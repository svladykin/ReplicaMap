package com.vladykin.replicamap.kafka.impl.serde;

import com.vladykin.replicamap.kafka.compute.ComputeSerializer;
import com.vladykin.replicamap.kafka.impl.msg.FlushNotification;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import com.vladykin.replicamap.kafka.impl.msg.MapUpdate;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiFunction;

import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_REQUEST;
import static com.vladykin.replicamap.kafka.impl.util.Utils.NULL_ARRAY_LENGTH;
import static com.vladykin.replicamap.kafka.impl.util.Utils.UUID_SIZE_BYTES;

/**
 * Operation message serializer.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class OpMessageSerializer<V> implements Serializer<OpMessage> {

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
        int opIdSize = ByteUtils.sizeOfVarlong(opMsg.getOpId());
        int expLen = Utils.getArrayLength(exp);
        int updLen = Utils.getArrayLength(upd);
        int funLen = Utils.getArrayLength(fun);
        int expLenSize = ByteUtils.sizeOfVarint(expLen);
        int updLenSize = ByteUtils.sizeOfVarint(updLen);
        int funLenSize = ByteUtils.sizeOfVarint(funLen);

        int resultLen = opTypeSize + UUID_SIZE_BYTES + opIdSize + expLenSize + updLenSize + funLenSize;

        if (expLen > 0)
            resultLen += expLen;
        if (updLen > 0)
            resultLen += updLen;
        if (funLen > 0)
            resultLen += funLen;

        byte[] result = new byte[resultLen];
        ByteBuffer buf = ByteBuffer.wrap(result);

        buf.put(opMsg.getOpType());
        Utils.writeUuid(opMsg.getClientId(), buf);
        ByteUtils.writeVarlong(opMsg.getOpId(), buf);
        Utils.writeByteArray(exp, buf);
        Utils.writeByteArray(upd, buf);
        Utils.writeByteArray(fun, buf);

        assert buf.remaining() == 0;
        return result;
    }

    protected byte[] serializeFlushRequest(FlushRequest flushMsg) {
        int opTypeSize = 1;
        int opsOffsetSize = ByteUtils.sizeOfVarlong(flushMsg.getOpsOffset());

        byte[] result = new byte[opTypeSize + UUID_SIZE_BYTES + opsOffsetSize];
        ByteBuffer buf = ByteBuffer.wrap(result);

        buf.put(flushMsg.getOpType());
        Utils.writeUuid(flushMsg.getClientId(), buf);
        ByteUtils.writeVarlong(flushMsg.getOpsOffset(), buf);

        assert buf.remaining() == 0;
        return result;
    }

    protected byte[] serializeFlushNotification(FlushNotification flushMsg) {
        int opTypeSize = 1;
        int flushOffsetOpsSize = ByteUtils.sizeOfVarlong(flushMsg.getOpsOffset());

        byte[] result = new byte[opTypeSize + UUID_SIZE_BYTES + flushOffsetOpsSize];
        ByteBuffer buf = ByteBuffer.wrap(result);

        buf.put(flushMsg.getOpType());
        Utils.writeUuid(flushMsg.getClientId(), buf);
        ByteUtils.writeVarlong(flushMsg.getOpsOffset(), buf);

        assert buf.remaining() == 0;
        return result;
    }

    @Override
    public void close() {
        Utils.close(valSer);
        Utils.close(funSer);
    }
}