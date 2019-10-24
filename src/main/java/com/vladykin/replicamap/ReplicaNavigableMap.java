package com.vladykin.replicamap;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;

/**
 * Replicated {@link NavigableMap} with async operations.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public interface ReplicaNavigableMap<K,V> extends ReplicaMap<K,V>, NavigableMap<K,V> {
    @Override
    NavigableMap<K,V> unwrap();

    @Override
    default Collection<V> values() {
        return ReplicaMap.super.values();
    }

    @Override
    default Set<Entry<K,V>> entrySet() {
        return ReplicaMap.super.entrySet();
    }

    @Override
    default Set<K> keySet() {
        return ReplicaMap.super.keySet();
    }

    @Override
    default Entry<K,V> lowerEntry(K key) {
        return unwrap().lowerEntry(key);
    }

    @Override
    default K lowerKey(K key) {
        return unwrap().lowerKey(key);
    }

    @Override
    default Entry<K,V> floorEntry(K key) {
        return unwrap().floorEntry(key);
    }

    @Override
    default K floorKey(K key) {
        return unwrap().floorKey(key);
    }

    @Override
    default Entry<K,V> ceilingEntry(K key) {
        return unwrap().ceilingEntry(key);
    }

    @Override
    default K ceilingKey(K key) {
        return unwrap().ceilingKey(key);
    }

    @Override
    default Entry<K,V> higherEntry(K key) {
        return unwrap().higherEntry(key);
    }

    @Override
    default K higherKey(K key) {
        return unwrap().higherKey(key);
    }

    @Override
    default Entry<K,V> firstEntry() {
        return unwrap().firstEntry();
    }

    @Override
    default Entry<K,V> lastEntry() {
        return unwrap().lastEntry();
    }

    @Override
    default Entry<K,V> pollFirstEntry() {
        for (;;) {
            Entry<K,V> first = firstEntry();

            if (first == null || remove(first.getKey(), first.getValue()))
                return first;
        }
    }

    @Override
    default Entry<K,V> pollLastEntry() {
        for (;;) {
            Entry<K,V> last = lastEntry();

            if (last == null || remove(last.getKey(), last.getValue()))
                return last;
        }
    }

    @Override
    default NavigableMap<K,V> descendingMap() {
        return Collections.unmodifiableNavigableMap(
            unwrap().descendingMap());
    }

    @Override
    default NavigableSet<K> navigableKeySet() {
        return Collections.unmodifiableNavigableSet(
            unwrap().navigableKeySet());
    }

    @Override
    default NavigableSet<K> descendingKeySet() {
        return Collections.unmodifiableNavigableSet(
            unwrap().descendingKeySet());
    }

    @Override
    default NavigableMap<K,V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return Collections.unmodifiableNavigableMap(
            unwrap().subMap(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    default NavigableMap<K,V> headMap(K toKey, boolean inclusive) {
        return Collections.unmodifiableNavigableMap(
            unwrap().headMap(toKey, inclusive));
    }

    @Override
    default NavigableMap<K,V> tailMap(K fromKey, boolean inclusive) {
        return Collections.unmodifiableNavigableMap(
            unwrap().tailMap(fromKey, inclusive));
    }

    @Override
    default SortedMap<K,V> subMap(K fromKey, K toKey) {
        return Collections.unmodifiableSortedMap(
            unwrap().subMap(fromKey, toKey));
    }

    @Override
    default SortedMap<K,V> headMap(K toKey) {
        return Collections.unmodifiableSortedMap(
            unwrap().headMap(toKey));
    }

    @Override
    default SortedMap<K,V> tailMap(K fromKey) {
        return Collections.unmodifiableSortedMap(
            unwrap().tailMap(fromKey));
    }

    @Override
    default Comparator<? super K> comparator() {
        return unwrap().comparator();
    }

    @Override
    default K firstKey() {
        return unwrap().firstKey();
    }

    @Override
    default K lastKey() {
        return unwrap().lastKey();
    }
}
