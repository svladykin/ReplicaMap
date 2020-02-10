package com.vladykin.replicamap.kafka.impl;

/**
 * Simple key-value record with offset.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class MiniRecord {
    protected final Object key;
    protected final Object value;
    protected final long offset;

    public MiniRecord(Object key, Object value, long offset) {
        this.key = key;
        this.value = value;
        this.offset = offset;
    }

    public Object key() {
        return key;
    }

    public Object value() {
        return value;
    }

    public long offset() {
        return offset;
    }

    @Override
    public String toString() {
        return "MiniRecord{" +
            "key=" + key +
            ", value=" + value +
            ", offset=" + offset +
            '}';
    }
}
