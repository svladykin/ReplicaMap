package com.vladykin.replicamap.kafka.impl.msg;

import com.vladykin.replicamap.kafka.compute.ComputeDeserializer;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiFunction;
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

    protected V readValue(String topic, ByteBuffer buf) {
        return read(topic, buf, valDes);
    }

    protected BiFunction<?,?,?> readFunction(String topic, ByteBuffer buf) {
        if (!buf.hasRemaining())
            return null; // Backward compatibility.

        return read(topic, buf, funDes);
    }

    protected <Z> Z read(String topic, ByteBuffer buf, Deserializer<Z> des) {
        byte[] arr = readByteArray(buf);

        if (arr == null)
            return null;

        if (des == null)
            throw new NullPointerException("Deserializer is not provided.");

        return des.deserialize(topic, arr);
    }

    @Override
    public OpMessage deserialize(String topic, byte[] opMsgBytes) {
        ByteBuffer buf = ByteBuffer.wrap(opMsgBytes);
        byte opType = buf.get();

        switch (opType) {
            case OP_FLUSH_REQUEST:
                return new FlushRequest(
                    ByteUtils.readVarlong(buf),
                    ByteUtils.readVarlong(buf),
                    ByteUtils.readVarlong(buf),
                    ByteUtils.readVarlong(buf));

            case OP_FLUSH_NOTIFICATION:
                return new FlushNotification(
                    ByteUtils.readVarlong(buf),
                    ByteUtils.readVarlong(buf),
                    ByteUtils.readVarlong(buf),
                    ByteUtils.readVarlong(buf));
        }

        return new MapUpdate(
            opType,
            ByteUtils.readVarlong(buf),
            ByteUtils.readVarlong(buf),
            readValue(topic, buf),
            readValue(topic, buf),
            readFunction(topic, buf)
        );
    }

    @Override
    public void close() {
        Utils.close(valDes);
        Utils.close(funDes);
    }
}
