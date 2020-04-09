package com.vladykin.replicamap.kafka.impl.msg;

import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Map update operation message.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class MapUpdateMessage extends OpMessage {

    protected final long opId;
    protected final Object expValue;
    protected final Object updValue;
    protected final BiFunction<?,?,?> function;

    public MapUpdateMessage(
        byte opType,
        long clientId,
        long opId,
        Object expValue,
        Object updValue,
        BiFunction<?,?,?> function
    ) {
        super(opType, clientId);

        this.opId = opId;
        this.expValue = expValue;
        this.updValue = updValue;
        this.function = function;
    }

    public long getOpId() {
        return opId;
    }

    public Object getExpectedValue() {
        return expValue;
    }

    public Object getUpdatedValue() {
        return updValue;
    }

    public BiFunction<?,?,?> getFunction() {
        return function;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MapUpdateMessage that = (MapUpdateMessage)o;

        if (opId != that.opId) return false;
        if (!Objects.equals(expValue, that.expValue)) return false;
        if (!Objects.equals(updValue, that.updValue)) return false;
        if (!Objects.equals(function, that.function)) return false;

        if (opType != that.opType) return false;
        return clientId == that.clientId;
    }

    @Override
    public int hashCode() {
        int result = opType;
        result = 31 * result + Long.hashCode(clientId);
        result = 31 * result + Long.hashCode(opId);
        result = 31 * result + (expValue != null ? expValue.hashCode() : 0);
        result = 31 * result + (updValue != null ? updValue.hashCode() : 0);
        result = 31 * result + (function != null ? function.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MapUpdateMessage{" +
            "opId=" + opId +
            ", expValue=" + expValue +
            ", updValue=" + updValue +
            ", function=" + function +
            ", clientId=" + Long.toHexString(clientId) +
            ", opType=" + (char)opType +
            '}';
    }
}
