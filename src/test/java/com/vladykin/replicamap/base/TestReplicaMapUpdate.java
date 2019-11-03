package com.vladykin.replicamap.base;

import java.util.Objects;
import java.util.function.BiFunction;

@SuppressWarnings("WeakerAccess")
public class TestReplicaMapUpdate<K,V> {
    public final long opId;
    public final byte updateType;
    public final K key;
    public final V exp;
    public final V upd;
    public final BiFunction<?,?,?> function;

    public final Object srcId;

    public TestReplicaMapUpdate(long opId, byte updateType, K key, V exp, V upd, BiFunction<?,?,?> function, Object srcId) {
        this.opId = opId;
        this.updateType = updateType;
        this.key = key;
        this.exp = exp;
        this.upd = upd;
        this.function = function;
        this.srcId = srcId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestReplicaMapUpdate<?,?> that = (TestReplicaMapUpdate<?,?>)o;

        if (updateType != that.updateType) return false;
        if (!key.equals(that.key)) return false;
        if (!Objects.equals(exp, that.exp)) return false;
        if (srcId.equals(that.srcId)) return false;
        return Objects.equals(upd, that.upd);
    }

    @Override
    public int hashCode() {
        int result = updateType;
        result = 31 * result + srcId.hashCode();
        result = 31 * result + key.hashCode();
        result = 31 * result + (exp != null ? exp.hashCode() : 0);
        result = 31 * result + (upd != null ? upd.hashCode() : 0);
        return result;
    }
}
