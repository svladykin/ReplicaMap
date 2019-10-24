package com.vladykin.replicamap;

import java.util.NavigableMap;

public class TestNavigableMap<K,V> extends TestMap<K,V> implements ReplicaNavigableMap<K,V> {
    public TestNavigableMap(NavigableMap<K,V> m) {
        super(m);
    }

    @Override
    public NavigableMap<K,V> unwrap() {
        return (NavigableMap<K,V>)super.unwrap();
    }
}
