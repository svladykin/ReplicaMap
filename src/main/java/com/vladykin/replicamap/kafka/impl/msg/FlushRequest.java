package com.vladykin.replicamap.kafka.impl.msg;

import java.util.Objects;
import java.util.UUID;

/**
 * Flush request message.
 *
 * @author Sergei Vladykin http://vladykin.com
 */
public class FlushRequest extends OpMessage {

    protected final long opsOffset;

    public FlushRequest(UUID clientId, long opsOffset) {
        super(OP_FLUSH_REQUEST, clientId);
        assert opsOffset >= 0;
        this.opsOffset = opsOffset;
    }

    public long getOpsOffset() {
        return opsOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlushRequest)) return false;

        FlushRequest that = (FlushRequest)o;

        return opsOffset == that.opsOffset &&
                opType == that.opType &&
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
        return "FlushRequest{" +
                "opType=" + (char)opType +
                ", clientId=" + clientId +
                ", opsOffset=" + opsOffset +
            '}';
    }


}
