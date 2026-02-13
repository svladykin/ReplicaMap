package com.vladykin.replicamap.kafka.impl.serde;

import com.vladykin.replicamap.kafka.compute.ComputeDeserializer;
import com.vladykin.replicamap.kafka.impl.msg.FlushNotification;
import com.vladykin.replicamap.kafka.impl.msg.FlushRequest;
import com.vladykin.replicamap.kafka.impl.msg.MapUpdate;
import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiFunction;

import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessage.OP_FLUSH_REQUEST;
import static com.vladykin.replicamap.kafka.impl.util.Utils.NULL_ARRAY_LENGTH;

/**
 * Operation message deserializer.
 *
 * @author Sergei Vladykin http://vladykin.com
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

    protected V readValue(String topic, ByteBuffer buf) {
        return read(topic, buf, valDes);
    }

    protected BiFunction<?,?,?> readFunction(String topic, ByteBuffer buf) {
        if (!buf.hasRemaining())
            return null; // Backward compatibility.

        return read(topic, buf, funDes);
    }

    protected <Z> Z read(String topic, ByteBuffer buf, Deserializer<Z> des) {
        byte[] arr = Utils.readByteArray(buf);

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
                    Utils.readUuid(buf),
                    ByteUtils.readVarlong(buf));

            case OP_FLUSH_NOTIFICATION:
                return new FlushNotification(
                    Utils.readUuid(buf),
                    ByteUtils.readVarlong(buf));
        }

        return new MapUpdate(
            opType,
            Utils.readUuid(buf),
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
