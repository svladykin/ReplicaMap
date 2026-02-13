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

    public FlushNotification(UUID clientId, long opsOffset) {
        super(OP_FLUSH_NOTIFICATION, clientId);
        this.opsOffset = opsOffset;
    }

    public long getOpsOffset() {
        return opsOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlushNotification)) return false;

        FlushNotification that = (FlushNotification)o;

        return opType == that.opType &&
                opsOffset == that.opsOffset &&
                Objects.equals(clientId, that.clientId);

    }

    @Override
    public int hashCode() {
        int result = opType;
        result = 31 * result + clientId.hashCode();
        result = 31 * result + Long.hashCode(opsOffset);
        return result;
    }

    @Override
    public String toString() {
        return "FlushNotification{" +
                "opType=" + (char)opType +
                ", clientId=" + clientId +
                ", opsOffset=" + opsOffset +
            '}';
    }
}
