package com.vladykin.replicamap.kafka.impl.msg;

/**
 * Notification about successful flush.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class FlushNotification extends OpMessage {

    protected final long flushOffsetData;
    protected final long flushOffsetOps;

    public FlushNotification(long clientId, long flushOffsetData, long flushOffsetOps) {
        super(OP_FLUSH_NOTIFICATION, clientId);

        this.flushOffsetData = flushOffsetData;
        this.flushOffsetOps = flushOffsetOps;
    }

    @SuppressWarnings("unused")
    FlushNotification(long clientId, long flushOffsetData, long flushOffsetOps, long ignore) { // Backward compatibility.
        this(clientId, flushOffsetData, flushOffsetOps);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlushNotification)) return false;

        FlushNotification that = (FlushNotification)o;

        if (flushOffsetData != that.flushOffsetData) return false;
        if (flushOffsetOps != that.flushOffsetOps) return false;

        if (opType != that.opType) return false;
        return clientId == that.clientId;
    }

    @Override
    public int hashCode() {
        int result = opType;
        result = 31 * result + Long.hashCode(clientId);
        result = 31 * result + Long.hashCode(flushOffsetData);
        result = 31 * result + Long.hashCode(flushOffsetOps);
        return result;
    }

    @Override
    public String toString() {
        return "FlushNotification{" +
            "flushOffsetData=" + flushOffsetData +
            ", flushOffsetOps=" + flushOffsetOps +
            ", clientId=" + Long.toHexString(clientId) +
            ", opType=" + (char)opType +
            '}';
    }
}
