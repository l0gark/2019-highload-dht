package ru.mail.polis.persistence;

import java.nio.ByteBuffer;

public final class Bytes {
    private Bytes() {
    }

    public static ByteBuffer fromInt(final int value) {
        final ByteBuffer result = ByteBuffer.allocate(Integer.BYTES);
        return result.putInt(value).rewind();
    }

    public static ByteBuffer fromLong(final long value) {
        final ByteBuffer result = ByteBuffer.allocate(Long.BYTES);
        return result.putLong(value).rewind();
    }
}
