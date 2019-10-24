package com.vladykin.replicamap.kafka.impl.msg;

import java.util.Objects;

public class OpMessage {
    protected final long clientId;
    protected final byte opType;
    protected final long opId;
    protected final Object expValue;
    protected final Object updValue;

    protected final long flushOffsetData;
    protected final long flushOffsetOps;
    protected final long cleanOffsetOps;

    public OpMessage(byte opType, long clientId, long opId, Object expValue, Object updValue) {
        this.opType = opType;
        this.clientId = clientId;
        this.opId = opId;
        this.expValue = expValue;
        this.updValue = updValue;

        flushOffsetData = 0L;
        flushOffsetOps = 0L;
        cleanOffsetOps = 0L;
    }

    public OpMessage(byte opType, long clientId, long flushOffsetData, long flushOffsetOps, long cleanOffsetOps) {
        this.opType = opType;
        this.clientId = clientId;
        this.flushOffsetData = flushOffsetData;
        this.flushOffsetOps = flushOffsetOps;
        this.cleanOffsetOps = cleanOffsetOps;

        opId = 0L;
        expValue = null;
        updValue = null;
    }

    /**
     * @return Offset of the last flushed data record.
     */
    public long getFlushOffsetData() {
        return flushOffsetData;
    }

    /**
     * @return Offset of the last flushed operation.
     */
    public long getFlushOffsetOps() {
        return flushOffsetOps;
    }

    /**
     * @return Offset of the last cleaned from the flush queue operation.
     */
    public long getCleanOffsetOps() {
        return cleanOffsetOps;
    }

    public long getOpId() {
        return opId;
    }

    public byte getOpType() {
        return opType;
    }

    public Object getExpectedValue() {
        return expValue;
    }

    public Object getUpdatedValue() {
        return updValue;
    }

    public long getClientId() {
        return clientId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OpMessage opMessage = (OpMessage)o;

        if (clientId != opMessage.clientId) return false;
        if (opType != opMessage.opType) return false;
        if (opId != opMessage.opId) return false;
        if (flushOffsetData != opMessage.flushOffsetData) return false;
        if (flushOffsetOps != opMessage.flushOffsetOps) return false;
        if (cleanOffsetOps != opMessage.cleanOffsetOps) return false;
        if (!Objects.equals(expValue, opMessage.expValue)) return false;
        return Objects.equals(updValue, opMessage.updValue);
    }

    @Override
    public int hashCode() {
        int result = (int)(clientId ^ (clientId >>> 32));
        result = 31 * result + (int)opType;
        result = 31 * result + (int)(opId ^ (opId >>> 32));
        result = 31 * result + (expValue != null ? expValue.hashCode() : 0);
        result = 31 * result + (updValue != null ? updValue.hashCode() : 0);
        result = 31 * result + (int)(flushOffsetData ^ (flushOffsetData >>> 32));
        result = 31 * result + (int)(flushOffsetOps ^ (flushOffsetOps >>> 32));
        result = 31 * result + (int)(cleanOffsetOps ^ (cleanOffsetOps >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "OpMessage{" +
            "clientId=" + Long.toHexString(clientId) +
            ", opType=" + (char)opType +
            ", opId=" + opId +
            ", expValue=" + expValue +
            ", updValue=" + updValue +
            ", flushOffsetData=" + flushOffsetData +
            ", flushOffsetOps=" + flushOffsetOps +
            ", cleanOffsetOps=" + cleanOffsetOps +
            '}';
    }
}
