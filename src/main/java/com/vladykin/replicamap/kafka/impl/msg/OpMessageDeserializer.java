package com.vladykin.replicamap.kafka.impl.msg;

import com.vladykin.replicamap.kafka.compute.ComputeDeserializer;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.ByteUtils;

import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_REQUEST;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessageSerializer.NULL_ARRAY_LENGTH;

/**
 * Operation message deserializer.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class OpMessageDeserializer<V> implements Deserializer<OpMessage> {
    protected final Deserializer<V> valDes;
    protected final ComputeDeserializer funDes;

    public OpMessageDeserializer(Deserializer<V> valDes, ComputeDeserializer funDes) {
        this.valDes = Utils.requireNonNull(valDes, "valDes");
        this.funDes = funDes;
    }

    @Override
    public void configure(Map<String,?> configs, boolean isKey) {
        valDes.configure(configs, isKey);

        if (funDes != null)
            funDes.configure(configs, isKey);
    }

    protected byte[] readByteArray(ByteBuffer buf) {
        int len = ByteUtils.readVarint(buf);

        if (len == NULL_ARRAY_LENGTH)
            return null;

        byte[] arr = new byte[len];

        if (len != 0)
            buf.get(arr);

        return arr;
    }

    protected V readValue(String topic, Headers headers, ByteBuffer buf) {
        return read(topic, headers, buf, valDes);
    }

    protected BiFunction<?,?,?> readFunction(String topic, Headers headers, ByteBuffer buf) {
        if (!buf.hasRemaining())
            return null; // compatibility

        return read(topic, headers, buf, funDes);
    }

    protected <Z> Z read(String topic, Headers headers, ByteBuffer buf, Deserializer<Z> des) {
        byte[] arr = readByteArray(buf);

        if (arr == null)
            return null;

        if (des == null)
            throw new NullPointerException("Deserializer is not provided.");

        return des.deserialize(topic, headers, arr);
    }

    @Override
    public OpMessage deserialize(String topic, byte[] opMsgBytes) {
        return deserialize(topic, null, opMsgBytes);
    }

    @Override
    public OpMessage deserialize(String topic, Headers headers, byte[] opMsgBytes) {
        ByteBuffer buf = ByteBuffer.wrap(opMsgBytes);
        byte opType = buf.get();

        if (opType == OP_FLUSH_REQUEST) {
            return new FlushRequest(
                ByteUtils.readVarlong(buf),
                ByteUtils.readVarlong(buf),
                ByteUtils.readVarlong(buf),
                ByteUtils.readVarlong(buf));
        }

        if (opType == OP_FLUSH_NOTIFICATION) {
            return new FlushNotification(
                ByteUtils.readVarlong(buf),
                ByteUtils.readVarlong(buf),
                ByteUtils.readVarlong(buf),
                ByteUtils.readVarlong(buf));
        }

        return new MapUpdateMessage(
            opType,
            ByteUtils.readVarlong(buf),
            ByteUtils.readVarlong(buf),
            readValue(topic, headers, buf),
            readValue(topic, headers, buf),
            readFunction(topic, headers, buf)
        );
    }

    @Override
    public void close() {
        Utils.close(valDes);
        Utils.close(funDes);
    }
}
