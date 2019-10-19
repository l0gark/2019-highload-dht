package ru.mail.polis.persistence;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

public interface Table {
    long sizeInBytes();

    @NotNull
    Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;

    void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value) throws IOException;

    void remove(@NotNull ByteBuffer key) throws IOException;

    /**
     * Dump to the file in directory.
     * List of Cells
     * -Cell
     * keySize - Integer
     * key - ByteBuffer sizeOf(key) = keySize.
     * Timestamp - time of last update Long
     * if timestamp is positive then next
     * valueSize - Integer
     * value - ByteBuffer sizeOf(value) = valueSize.
     * -offsets LongBuffer
     * -count rows Long
     *
     * @param cells iterator of data
     * @param to    directory
     * @throws IOException If an I/O error occurs
     */
    static void write(final Iterator<Cell> cells, final File to) throws IOException {
        try (FileChannel fc = FileChannel.open(to.toPath(),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            final List<Long> offsets = new ArrayList<>();
            long offset = 0;
            while (cells.hasNext()) {
                offsets.add(offset);

                final Cell cell = cells.next();
                final ByteBuffer key = cell.getKey();
                final int keySize = cell.getKey().remaining();

                final Value value = cell.getValue();

                final int bufferSize = Integer.BYTES
                        + key.remaining()
                        + Long.BYTES
                        + (value.isRemoved() ? 0 : Integer.BYTES + value.getData().remaining());

                final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
                buffer.putInt(keySize).put(key);

                // Timestamp
                if (value.isRemoved()) {
                    buffer.putLong(-cell.getValue().getTimeStamp());
                } else {
                    buffer.putLong(cell.getValue().getTimeStamp());
                }

                // Value
                if (!value.isRemoved()) {
                    final ByteBuffer valueData = value.getData();
                    final int valueSize = valueData.remaining();
                    buffer.putInt(valueSize).put(valueData);
                }

                buffer.flip();
                fc.write(buffer);
                offset += bufferSize;
            }

            // Offsets
            for (final long anOffset : offsets) {
                fc.write(Bytes.fromLong(anOffset));
            }

            // Rows
            fc.write(Bytes.fromLong(offsets.size()));
        }
    }
}
