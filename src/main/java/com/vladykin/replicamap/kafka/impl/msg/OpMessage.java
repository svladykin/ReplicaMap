package com.vladykin.replicamap.kafka.impl.msg;

/**
 * Operation message base class.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public abstract class OpMessage {
    protected final long clientId;
    protected final byte opType;

    public OpMessage(byte opType, long clientId) {
        this.opType = opType;
        this.clientId = clientId;
    }

    public byte getOpType() {
        return opType;
    }

    public long getClientId() {
        return clientId;
    }
}
