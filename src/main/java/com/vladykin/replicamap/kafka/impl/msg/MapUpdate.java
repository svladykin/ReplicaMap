package com.vladykin.replicamap.kafka.impl.msg;

import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * Map update operation message.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class MapUpdate extends OpMessage {

    protected final long opId;
    protected final Object expValue;
    protected final Object updValue;
    protected final BiFunction<?,?,?> function;

    public MapUpdate(
        byte opType,
        UUID clientId,
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

        MapUpdate that = (MapUpdate)o;

        return opId == that.opId &&
                opType == that.opType &&
                Objects.equals(clientId, that.clientId) &&
                Objects.equals(expValue, that.expValue) &&
                Objects.equals(updValue, that.updValue) &&
                Objects.equals(function, that.function);

    }

    @Override
    public int hashCode() {
        int result = opType;
        result = 31 * result + clientId.hashCode();
        result = 31 * result + Long.hashCode(opId);
        result = 31 * result + (expValue != null ? expValue.hashCode() : 0);
        result = 31 * result + (updValue != null ? updValue.hashCode() : 0);
        result = 31 * result + (function != null ? function.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MapUpdateMessage{" +
                "opType=" + (char)opType +
                ", clientId=" + clientId +
                ", opId=" + opId +
                ", expValue=" + expValue +
                ", updValue=" + updValue +
                ", function=" + function +
            '}';
    }
}
