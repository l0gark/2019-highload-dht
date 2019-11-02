package ru.mail.polis.persistence;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public final class Value implements Comparable<Value> {
    private static final Value ABSENT = new Value(State.ABSENT, 0, null);

    private final long ts;
    private final ByteBuffer data;
    private final State state;
    private static final AtomicInteger nano = new AtomicInteger();
    private static final int FACTOR = 1_000_000;

    /**
     * Value with state.
     *
     * @param state of value
     * @param ts timestamp
     * @param data bytebuffer
     */
    public Value(State state, final long ts, final ByteBuffer data) {
        assert ts >= 0;
        this.state = state;
        this.ts = ts;
        this.data = data;
    }

    /**
     * Create Value and put ts from nano.
     *
     * @param data data marked by ts
     * @return and go back
     */
    public static Value of(final ByteBuffer data) {
        return new Value(State.PRESENT, getMoment(), data.duplicate());
    }

    public static Value of(final long time, final ByteBuffer data) {
        return new Value(State.PRESENT, time, data.duplicate());
    }

    @NotNull
    public static Value absent() {
        return ABSENT;
    }

    public static Value tombstone() {
        return tombstone(getMoment());
    }

    public static Value tombstone(final long time) {
        return new Value(State.REMOVED, time, null);
    }

    public boolean isRemoved() {
        return data == null;
    }

    /**
     * Get data if exist.
     * @return data
     */
    public ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException("");
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value value) {
        return Long.compare(value.ts, ts);
    }

    public long getTimeStamp() {
        return ts;
    }

    private static long getMoment() {
        final long time = System.currentTimeMillis() * FACTOR + nano.incrementAndGet();
        if (nano.get() > FACTOR) {
            nano.set(0);
        }
        return time;
    }

    public State state() {
        return state;
    }

    @Override
    public String toString() {
        return state.toString() + ", ts=" + ts;
    }

    /**
     * Merge values by timestamp.
     *
     * @param values list of values
     * @return min value or absent
     */
    @NotNull
    public static Value merge(@NotNull final Collection<Value> values) {
        return values.stream()
                .filter(v -> v.state() != Value.State.ABSENT)
                .min(Value::compareTo)
                .orElseGet(Value::absent);
    }

    public enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }
}
