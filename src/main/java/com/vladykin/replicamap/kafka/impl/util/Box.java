package com.vladykin.replicamap.kafka.impl.util;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Simple reference.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public class Box<X> implements Supplier<X>, Consumer<X> {
    protected X value;

    public Box() {
        // no-op
    }

    public void set(X value) {
        this.value = value;
    }

    public void clear() {
        this.value = null;
    }

    @Override
    public X get() {
        return value;
    }

    @Override
    public void accept(X x) {
        set(x);
    }

    @Override
    public String toString() {
        return "Box{" +
            "value=" + value +
            '}';
    }
}
