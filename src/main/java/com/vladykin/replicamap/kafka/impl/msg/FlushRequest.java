package com.vladykin.replicamap.kafka.impl.msg;

/**
 * Flush request message.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class FlushRequest extends OpMessage {

    protected final long flushOffsetOps;
    protected final long cleanOffsetOps;

    public FlushRequest(long clientId, long flushOffsetOps, long cleanOffsetOps) {
        super(OP_FLUSH_REQUEST, clientId);

        this.flushOffsetOps = flushOffsetOps;
        this.cleanOffsetOps = cleanOffsetOps;
    }

    @SuppressWarnings("unused")
    FlushRequest(long clientId, long ignore, long flushOffsetOps, long cleanOffsetOps) { // Backward compatibility.
        this(clientId, flushOffsetOps, cleanOffsetOps);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlushRequest)) return false;

        FlushRequest that = (FlushRequest)o;

        if (flushOffsetOps != that.flushOffsetOps) return false;
        if (cleanOffsetOps != that.cleanOffsetOps) return false;

        if (opType != that.opType) return false;
        return clientId == that.clientId;
    }

    @Override
    public int hashCode() {
        int result = opType;
        result = 31 * result + Long.hashCode(clientId);
        result = 31 * result + Long.hashCode(flushOffsetOps);
        result = 31 * result + Long.hashCode(cleanOffsetOps);
        return result;
    }

    @Override
    public String toString() {
        return "FlushRequest{" +
            "flushOffsetOps=" + flushOffsetOps +
            ", cleanOffsetOps=" + cleanOffsetOps +
            ", clientId=" + Long.toHexString(clientId) +
            ", opType=" + (char)opType +
            '}';
    }
}
