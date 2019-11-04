package com.vladykin.replicamap.base;

import com.vladykin.replicamap.ReplicaMapListener;
import java.util.function.BiFunction;

/**
 * {@link ReplicaMapBase} itself can not always reliably detect if the function
 * has modified the value or not. This may happen when you modify the value inplace
 * instead of creating a modified copy. In this case {@link ReplicaMapListener}
 * will not be invoked.
 *
 * If the function implements this interface {@link ReplicaMapBase} can ask the
 * update status of the last invocation using the method {@link #wasUpdated()},
 * otherwise {@link ReplicaMapBase} will just check the old and the new values for equality
 * to decide if we need to fire {@link ReplicaMapListener}.
 *
 * @see ReplicaMapBase#compute(Object, BiFunction)
 * @see ReplicaMapBase#computeIfPresent(Object, BiFunction)
 * @see ReplicaMapBase#merge(Object, Object, BiFunction)
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public interface WasUpdated {
    /**
     * @return {@code true} If the last execution of this function updated the value.
     */
    boolean wasUpdated();
}
