package com.vladykin.replicamap.kafka.impl.msg;

import java.util.Objects;
import java.util.UUID;

/**
 * Notification message about successful flush.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class FlushNotification extends OpMessage {

    protected final long opsOffset;

    /** Offset of the last data-topic record written by this flush (used to validate the resume point on load). */
    protected final long dataOffset;

    public FlushNotification(UUID clientId, long opsOffset, long dataOffset) {
        super(OP_FLUSH_NOTIFICATION, clientId);
        this.opsOffset = opsOffset;
        this.dataOffset = dataOffset;
    }

    public long getOpsOffset() {
        return opsOffset;
    }

    public long getDataOffset() {
        return dataOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlushNotification)) return false;

        FlushNotification that = (FlushNotification)o;

        return opType == that.opType &&
                opsOffset == that.opsOffset &&
                dataOffset == that.dataOffset &&
                Objects.equals(clientId, that.clientId);

    }

    @Override
    public int hashCode() {
        int result = opType;
        result = 31 * result + clientId.hashCode();
        result = 31 * result + Long.hashCode(opsOffset);
        result = 31 * result + Long.hashCode(dataOffset);
        return result;
    }

    @Override
    public String toString() {
        return "FlushNotification{" +
                "opType=" + (char)opType +
                ", clientId=" + clientId +
                ", opsOffset=" + opsOffset +
                ", dataOffset=" + dataOffset +
            '}';
    }
}
