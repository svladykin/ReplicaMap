package com.vladykin.replicamap.base;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapListener;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vladykin.replicamap.base.ReplicaMapBase.OpState.FINISHED;
import static com.vladykin.replicamap.base.ReplicaMapBase.OpState.SENDING;
import static com.vladykin.replicamap.base.ReplicaMapBase.OpState.STARTING;

/**
 * Base class for implementing {@link ReplicaMap} with abstract communication.
 *
 * @see #sendUpdate
 * @see #onReceiveUpdate
 * @see #canSendFunction(BiFunction)
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public abstract class ReplicaMapBase<K, V> implements ReplicaMap<K, V> {
    private static final Logger log = LoggerFactory.getLogger(ReplicaMapBase.class);

    public static final byte OP_PUT = 'p';
    public static final byte OP_PUT_IF_ABSENT = 'P';

    public static final byte OP_REPLACE_ANY = 'c';
    public static final byte OP_REPLACE_EXACT = 'C';

    public static final byte OP_REMOVE_ANY = 'r';
    public static final byte OP_REMOVE_EXACT = 'R';

    public static final byte OP_COMPUTE = 'x';
    public static final byte OP_COMPUTE_IF_PRESENT = 'X';

    public static final byte OP_MERGE = 'm';

    public static final byte OP_FLUSH_REQUEST = 'f';
    public static final byte OP_FLUSH_NOTIFICATION = 'F';

    protected static final AtomicReferenceFieldUpdater<ReplicaMapBase, ReplicaMapListener> LISTENER =
        AtomicReferenceFieldUpdater.newUpdater(ReplicaMapBase.class, ReplicaMapListener.class, "listener");

    protected static final AtomicLongFieldUpdater<ReplicaMapBase> LAST_OP_ID =
        AtomicLongFieldUpdater.newUpdater(ReplicaMapBase.class, "lastOpId");

    protected final Object id;
    protected final Map<K,V> map;

    protected final ConcurrentMap<OpKey<K>,AsyncOp<Object,?,?>> ops = new ConcurrentHashMap<>();
    protected volatile long lastOpId;
    protected final Semaphore opsSemaphore;
    protected final long sendTimeout;
    protected final TimeUnit timeUnit;

    protected volatile ReplicaMapListener<K,V> listener;

    public ReplicaMapBase(Object id, Map<K,V> map, Semaphore opsSemaphore, long sendTimeout, TimeUnit timeUnit) {
        this.id = id;
        this.map = Utils.requireNonNull(map, "map");
        this.opsSemaphore = Utils.requireNonNull(opsSemaphore, "opsSemaphore");
        this.sendTimeout = sendTimeout;
        this.timeUnit = Utils.requireNonNull(timeUnit, "timeUnit");
    }

    @Override
    public Object id() {
        return id;
    }

    @Override
    public ReplicaMapListener<K,V> getListener() {
        return listener;
    }

    @Override
    public void setListener(ReplicaMapListener<K,V> listener) {
        this.listener = listener;
    }

    @Override
    public boolean casListener(ReplicaMapListener<K,V> expected, ReplicaMapListener<K,V> newListener) {
        return LISTENER.compareAndSet(this, expected, newListener);
    }

    @Override
    public Map<K,V> unwrap() {
        return map;
    }

    @Override
    public CompletableFuture<V> asyncPut(K key, V value) {
        return new Put<>(this, key, value).start();
    }

    @Override
    public CompletableFuture<V> asyncPutIfAbsent(K key, V value) {
        return new PutIfAbsent<>(this, key, value).start();
    }

    @Override
    public CompletableFuture<Boolean> asyncReplace(K key, V oldValue, V newValue) {
        return new ReplaceExact<>(this, key, oldValue, newValue).start();
    }

    @Override
    public CompletableFuture<V> asyncReplace(K key, V value) {
        return new ReplaceAny<>(this, key, value).start();
    }

    @Override
    public CompletableFuture<V> asyncRemove(K key) {
        return new RemoveAny<>(this, key).start();
    }

    @Override
    public CompletableFuture<Boolean> asyncRemove(K key, V value) {
        return new RemoveExact<>(this, key, value).start();
    }

    @Override
    public CompletableFuture<V> asyncCompute(K key, BiFunction<? super K,? super V,? extends V> remappingFunction) {
        checkCanSendFunction(remappingFunction);
        return new Compute<>(this, key, remappingFunction).start();
    }

    @Override
    public CompletableFuture<V> asyncComputeIfPresent(
        K key,
        BiFunction<? super K,? super V,? extends V> remappingFunction
    ) {
        checkCanSendFunction(remappingFunction);
        return new ComputeIfPresent<>(this, key, remappingFunction).start();
    }

    @Override
    public CompletableFuture<V> asyncMerge(
        K key,
        V value,
        BiFunction<? super V,? super V,? extends V> remappingFunction
    ) {
        checkCanSendFunction(remappingFunction);
        return new Merge<>(this, key, value, remappingFunction).start();
    }

    @Override
    public V compute(K key, BiFunction<? super K,? super V,? extends V> remappingFunction) {
        if (canSendNonNullFunction(remappingFunction)) {
            try {
                return asyncCompute(key, remappingFunction).get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new ReplicaMapException(e);
            }
        }
        return ReplicaMap.super.compute(key, remappingFunction);
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K,? super V,? extends V> remappingFunction) {
        if (canSendNonNullFunction(remappingFunction)) {
            try {
                return asyncComputeIfPresent(key, remappingFunction).get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new ReplicaMapException(e);
            }
        }
        return ReplicaMap.super.computeIfPresent(key, remappingFunction);
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V,? super V,? extends V> remappingFunction) {
        if (canSendNonNullFunction(remappingFunction)) {
            try {
                return asyncMerge(key, value, remappingFunction).get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new ReplicaMapException(e);
            }
        }
        return ReplicaMap.super.merge(key, value, remappingFunction);
    }

    @Override
    public void replaceAll(BiFunction<? super K,? super V,? extends V> remappingFunction) {
        if (canSendNonNullFunction(remappingFunction)) {
            try {
                asyncReplaceAll(remappingFunction).get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new ReplicaMapException(e);
            }
        }
        else
            ReplicaMap.super.replaceAll(remappingFunction);
    }

    /**
     * Must be called by the external processor of the updates queue.
     *
     * Each call is not necessary will result in the actual map update because
     * state may asynchronously change and precondition may fail.
     *
     * This method can be called from multiple threads simultaneously, but
     * for the same key there must be no parallel invocations.
     *
     * @param myUpdate {@code true} If this update was issued by this map instance,
     *                 or {@code false} if it is a remote update.
     * @param opId Local operation id.
     * @param updateType Update type.
     * @param key Key.
     * @param exp Expected value or {@code null} if none.
     * @param upd New value or {@code null} if none.
     * @param function Function to apply.
     * @param updatedValueConsumer Consume the updated value.
     * @return {@code true} If the map was actually updated, {@code false} if not.
     */
    @SuppressWarnings({"unchecked", "UnusedReturnValue"})
    public boolean onReceiveUpdate(
        boolean myUpdate,
        long opId,
        byte updateType,
        K key,
        V exp,
        V upd,
        BiFunction<?,?,?> function,
        Consumer<V> updatedValueConsumer
    ) {
        Object result = null;
        Throwable ex = null;
        boolean updated;
        V old;

        Map<K,V> m = map;

        try {
            switch (updateType) {
                case OP_PUT:
                    result = m.put(key, upd);
                    old = (V)result;
                    updated = true;
                    break;

                case OP_PUT_IF_ABSENT:
                    result = m.putIfAbsent(key, upd);
                    old = (V)result;
                    updated = old == null;
                    break;

                case OP_REPLACE_EXACT:
                    result = m.replace(key, exp, upd);
                    old = exp;
                    updated = (boolean)result;
                    break;

                case OP_REPLACE_ANY:
                    result = m.replace(key, upd);
                    old = (V)result;
                    updated = old != null;
                    break;

                case OP_REMOVE_ANY:
                    result = m.remove(key);
                    old = (V)result;
                    updated = old != null;
                    break;

                case OP_REMOVE_EXACT:
                    result = m.remove(key, exp);
                    old = exp;
                    updated = (boolean)result;
                    break;

                case OP_COMPUTE:
                    old = m.get(key);
                    result = m.compute(key, (BiFunction<? super K,? super V,? extends V>)function);
                    upd = (V)result;
                    updated = wasUpdated(old, upd, function);
                    break;

                case OP_COMPUTE_IF_PRESENT:
                    old = m.get(key);
                    result = m.computeIfPresent(key, (BiFunction<? super K,? super V,? extends V>)function);
                    upd = (V)result;
                    updated = wasUpdated(old, upd, function);
                    break;

                case OP_MERGE:
                    old = m.get(key);
                    result = m.merge(key, upd, (BiFunction<? super V,? super V,? extends V>)function);
                    upd = (V)result;
                    updated = wasUpdated(old, upd, function);
                    break;

                default:
                    assert !myUpdate;
                    log.warn("Unexpected op type: {}", (char)updateType);
                    old = null;
                    result = null;
                    updated = false;
            }
        }
        catch (Exception e) {
            ex = e;
            log.error("Failed to apply received update for key: " + key, e);
            throw new ReplicaMapException("Unrecoverable error in map: " + id, e);
        }
        finally {
            if (myUpdate) {
                AsyncOp<Object,?,?> op = ops.get(new OpKey<>(key, opId));
                if (op != null)
                    op.finish(result, ex, true);
                else
                    log.warn("AsyncOp was not found for key [{}] and op id: {}", key, opId);
            }
        }

        if (updated) {
            onMapUpdate(myUpdate, key, old, upd);

            if (updatedValueConsumer != null)
                updatedValueConsumer.accept(upd);
        }

        return updated;
    }

    @SuppressWarnings("unused")
    protected boolean wasUpdated(Object oldValue, Object newValue, BiFunction<?,?,?> function) {
        return !Objects.equals(oldValue, newValue);
    }

    protected void onMapUpdate(boolean myUpdate, K key, V old, V upd) {
        ReplicaMapListener<K,V> lsnr = getListener();

        if (lsnr != null) {
            try {
                lsnr.onMapUpdate(this, myUpdate, key, old, upd);
            }
            catch (Exception e) {
                log.error("Listener failed.", e);
            }
        }
    }

    protected void doSendUpdate(AsyncOp<?,K,V> op) {
        try {
            sendUpdate(op.opKey.opId, op.updateType, op.opKey.key, op.exp, op.upd, op.function, op);
        }
        catch (Exception e) {
            op.onError(e);
        }
    }

    protected void checkCanSendFunction(BiFunction<?,?,?> remappingFunction) {
        if (!canSendNonNullFunction(remappingFunction))
            throw new ReplicaMapException(
                "Async API is unavailable for functions that can not be sent: " + remappingFunction);
    }

    protected void beforeStart(AsyncOp<?,K,V> op) {
        // no-op
    }

    protected void beforeTryRun(AsyncOp<?,K,V> op) {
        // no-op
    }

    protected ReplicaMapException wrapOpError(AsyncOp<?,K,V> op, Throwable th, K key) {
        return new ReplicaMapException(op.getClass().getSimpleName() + " failed on key: " + key, th);
    }

    public static void interruptRunningOps(ReplicaMap<?,?> map) {
        if (map instanceof ReplicaMapBase)
            ((ReplicaMapBase<?, ?>)map).interruptRunningOps();
    }

    public void interruptRunningOps() {
        Exception e = new InterruptedException();
        ops.values().forEach(op -> op.onError(e));
    }

    /**
     * @param opId Operation id.
     * @param updateType Update type.
     * @param key Key.
     * @param exp Expected value or {@code null} if none.
     * @param upd New value or {@code null} if none.
     * @param function Function to send.
     * @param onSendFailed Callback for asynchronous send failure handling.
     * @throws Exception If failed.
     */
    protected abstract void sendUpdate(
        long opId,
        byte updateType,
        K key,
        V exp,
        V upd,
        BiFunction<?,?,?> function,
        FailureCallback onSendFailed
    ) throws Exception;

    /**
     * @param function Function to send.
     * @return {@code true} If this map is able to send the given function to other replicas.
     */
    protected abstract boolean canSendFunction(BiFunction<?,?,?> function);

    protected boolean canSendNonNullFunction(BiFunction<?,?,?> remappingFunction) {
        Utils.requireNonNull(remappingFunction, "remappingFunction");
        return canSendFunction(remappingFunction);
    }

    protected long nextOpId() {
        return LAST_OP_ID.incrementAndGet(this);
    }

    protected boolean acquirePermit(AsyncOp<?,?,?> op) {
        Exception ex;

        try {
            if (opsSemaphore.tryAcquire(sendTimeout, timeUnit))
                return true;

            ex = new TimeoutException();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ex = e;
        }

        op.finish(null, ex, false);
        return false;
    }

    @Override
    public String toString() {
        return "ReplicaMapBase{" +
            "id=" + id +
            ", mapSize=" + map.size() +
            ", opsSize=" + ops.size() +
            ", lastOpId=" + lastOpId +
            ", opsSemaphorePermits=" + opsSemaphore.availablePermits() +
            ", opsSemaphoreQueue=" + opsSemaphore.getQueueLength() +
            ", sendTimeout=" + sendTimeout +
            ", timeUnit=" + timeUnit +
            ", listener=" + listener +
            '}';
    }

    protected static abstract class AsyncOp<R,K,V> extends CompletableFuture<R> implements FailureCallback {
        protected static final AtomicReferenceFieldUpdater<AsyncOp, OpState> STATE =
            AtomicReferenceFieldUpdater.newUpdater(AsyncOp.class, OpState.class, "state");

        protected final OpKey<K> opKey;
        protected final byte updateType;
        protected final V exp;
        protected final V upd;
        protected final BiFunction<?,?,?> function;

        protected volatile OpState state;
        protected final ReplicaMapBase<K,V> map;

        public AsyncOp(
            ReplicaMapBase<K,V> map,
            byte updateType,
            K key,
            V exp,
            V upd,
            BiFunction<?,?,?> function
        ) {
            Utils.requireNonNull(key, "key");

            this.map = map;
            this.updateType = updateType;
            this.exp = exp;
            this.upd = upd;
            this.function = function;
            opKey = new OpKey<>(key, map.nextOpId());
        }

        protected boolean casState(OpState exp, OpState upd) {
            return STATE.compareAndSet(this, exp, upd);
        }

        protected abstract boolean checkPrecondition(boolean releaseOnFail);

        public AsyncOp<R,K,V> start() {
            try {
                map.beforeStart(this);
            }
            catch (Exception e) {
                finish(null, e, false);
                return this;
            }

            state = STARTING;

            if (checkPrecondition(false) && map.acquirePermit(this))
                tryRun();

            return this;
        }

        @SuppressWarnings("unchecked")
        protected void tryRun() {
            map.beforeTryRun(this);

            if (!checkPrecondition(true))
                return;

            // isDone means here that the future was either cancelled or exceptionally completed some other way.
            if (Thread.currentThread().isInterrupted() || isDone()) {
                onError(new InterruptedException());
                return;
            }

            // It is safe if someone cancels the op right here, because cancel does not call finish().
            // Thus we have to continue sending the update.

            map.ops.put(opKey, (AsyncOp<Object,?,?>)this);
            casState(STARTING, SENDING);
            map.doSendUpdate(this);
        }

        @Override
        public void onError(Throwable th) {
            finish(null, th, true);
        }

        public void finish(R result, Throwable error, boolean release) {
            for (;;) {
                OpState s = state;

                if (s == FINISHED)
                    return;

                if (casState(s, FINISHED))
                    break;
            }

            if (release) {
                map.ops.remove(opKey, this);
                map.opsSemaphore.release();
            }

            if (error == null)
                complete(result);
            else if (!isDone())
                completeExceptionally(map.wrapOpError(this, error, opKey.key));
        }
    }

    protected enum OpState {
        STARTING, SENDING, FINISHED
    }

    protected static class Put<K,V> extends AsyncOp<V,K,V> {
        public Put(ReplicaMapBase<K,V> m, K key, V value) {
            super(m, OP_PUT, key, null, Utils.requireNonNull(value, "value"), null);
        }

        @Override
        protected boolean checkPrecondition(boolean releaseOnFail) {
            V v = map.map.get(opKey.key);

            if (!upd.equals(v))
                return true;

            finish(v, null, releaseOnFail);
            return false;
        }
    }

    protected static class PutIfAbsent<K,V> extends AsyncOp<V,K,V> {
        public PutIfAbsent(ReplicaMapBase<K,V> m, K key, V value) {
            super(m, OP_PUT_IF_ABSENT, key, null, Utils.requireNonNull(value, "value"), null);
        }

        @Override
        protected boolean checkPrecondition(boolean releaseOnFail) {
            V v = map.map.get(opKey.key);

            if (v == null)
                return true;

            finish(v, null, releaseOnFail);
            return false;
        }
    }

    protected static class ReplaceExact<K,V> extends AsyncOp<Boolean,K,V> {
        public ReplaceExact(ReplicaMapBase<K,V> m, K key, V oldValue, V newValue) {
            super(m, OP_REPLACE_EXACT, key,
                Utils.requireNonNull(oldValue, "oldValue"),
                Utils.requireNonNull(newValue, "newValue"),
                null);
        }

        @Override
        protected boolean checkPrecondition(boolean releaseOnFail) {
            if (exp.equals(map.map.get(opKey.key)))
                return true;

            finish(Boolean.FALSE, null, releaseOnFail);
            return false;
        }
    }

    protected static class ReplaceAny<K,V> extends AsyncOp<V,K,V> {
        public ReplaceAny(ReplicaMapBase<K,V> m, K key, V value) {
            super(m, OP_REPLACE_ANY, key, null, Utils.requireNonNull(value, "value"), null);
        }

        @Override
        protected boolean checkPrecondition(boolean releaseOnFail) {
            if (map.map.containsKey(opKey.key))
                return true;

            finish(null, null, releaseOnFail);
            return false;
        }
    }

    protected static class RemoveAny<K,V> extends AsyncOp<V,K,V> {
        public RemoveAny(ReplicaMapBase<K,V> m, K key) {
            super(m, OP_REMOVE_ANY, key, null, null, null);
        }

        @Override
        protected boolean checkPrecondition(boolean releaseOnFail) {
            if (map.map.containsKey(opKey.key))
                return true;

            finish(null, null, releaseOnFail);
            return false;
        }
    }

    protected static class RemoveExact<K,V> extends AsyncOp<Boolean,K,V> {
        public RemoveExact(ReplicaMapBase<K,V> m, K key, V value) {
            super(m, OP_REMOVE_EXACT, key, Utils.requireNonNull(value, "value"), null, null);
        }

        @Override
        protected boolean checkPrecondition(boolean releaseOnFail) {
            if (exp.equals(map.map.get(opKey.key)))
                return true;

            finish(Boolean.FALSE, null, releaseOnFail);
            return false;
        }
    }

    protected static class Compute<K,V> extends AsyncOp<V,K,V> {
        public Compute(
            ReplicaMapBase<K,V> map,
            K key,
            BiFunction<? super K, ? super V, ? extends V> remappingFunction
        ) {
            super(map, OP_COMPUTE, key, null, null,
                Utils.requireNonNull(remappingFunction, "remappingFunction"));
        }

        @Override
        protected boolean checkPrecondition(boolean releaseOnFail) {
            return true;
        }
    }

    protected static class ComputeIfPresent<K,V> extends AsyncOp<V,K,V> {
        public ComputeIfPresent(
            ReplicaMapBase<K,V> map,
            K key,
            BiFunction<? super K, ? super V, ? extends V> remappingFunction
        ) {
            super(map, OP_COMPUTE_IF_PRESENT, key, null, null,
                Utils.requireNonNull(remappingFunction, "remappingFunction"));
        }

        @Override
        protected boolean checkPrecondition(boolean releaseOnFail) {
            if (map.map.containsKey(opKey.key))
                return true;

            finish(null, null, releaseOnFail);
            return false;
        }
    }

    protected static class Merge<K,V> extends AsyncOp<V,K,V> {
        public Merge(
            ReplicaMapBase<K,V> map,
            K key,
            V value,
            BiFunction<?,?,?> remappingFunction
        ) {
            super(map, OP_MERGE, key, null,
                Utils.requireNonNull(value, "value"),
                Utils.requireNonNull(remappingFunction, "remappingFunction"));
        }

        @Override
        protected boolean checkPrecondition(boolean releaseOnFail) {
            return true;
        }
    }

    protected static class OpKey<K> {
        final K key;
        final long opId;

        public OpKey(K key, long opId) {
            this.key = key;
            this.opId = opId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            OpKey<?> opKey = (OpKey<?>)o;
            return opId == opKey.opId && key.equals(opKey.key);
        }

        @Override
        public int hashCode() {
            int result = key.hashCode();
            result = 31 * result + (int)(opId ^ (opId >>> 32));
            return result;
        }
    }
}
