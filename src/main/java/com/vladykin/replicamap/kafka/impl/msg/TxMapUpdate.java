package com.vladykin.replicamap.kafka.impl.msg;

import java.util.UUID;

public class TxMapUpdate extends OpMessage {

    protected final UUID txId;

    public TxMapUpdate(UUID clientId, long opId, Object expValue, Object updValue, UUID txId, short[] parts, short[] keys) {
        super(OP_TX, clientId);

        this.txId = txId;
    }

    public UUID getTxId() {
        return txId;
    }
}
