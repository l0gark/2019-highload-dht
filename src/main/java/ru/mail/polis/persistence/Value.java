package ru.mail.polis.persistence;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public final class Value implements Comparable<Value> {
    private final long ts;
    private final ByteBuffer data;
    private static final AtomicInteger nano = new AtomicInteger();
    private static final int FACTOR = 1_000_000;

    private Value(final long ts, final ByteBuffer data) {
        assert ts >= 0;
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
        return new Value(getMoment(), data.duplicate());
    }

    public static Value of(final long time, final ByteBuffer data) {
        return new Value(time, data.duplicate());
    }

    static Value tombstone() {
        return tombstone(getMoment());
    }

    static Value tombstone(final long time) {
        return new Value(time, null);
    }

    boolean isRemoved() {
        return data == null;
    }

    ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException("");
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value value) {
        return Long.compare(value.ts, ts);
    }

    long getTimeStamp() {
        return ts;
    }

    private static long getMoment() {
        final long time = System.currentTimeMillis() * FACTOR + nano.incrementAndGet();
        if (nano.get() > FACTOR) {
            nano.set(0);
        }
        return time;
    }
}
