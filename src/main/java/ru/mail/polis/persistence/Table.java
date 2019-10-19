package ru.mail.polis.persistence;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
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
    static void write(@NotNull final Iterator<Cell> cells, @NotNull final File to)
            throws IOException {
        try (FileChannel fileChannel = FileChannel.open(
                to.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final List<Long> offsets = new ArrayList<>();
            long offset = 0;
            while (cells.hasNext()) {
                offsets.add(offset);

                final Cell cell = cells.next();

                final ByteBuffer key = cell.getKey();
                final int keySize = cell.getKey().remaining();
                fileChannel.write(Bytes.fromInt(keySize));
                offset += Integer.BYTES;
                final ByteBuffer keyDuplicate = key.duplicate();
                fileChannel.write(keyDuplicate);
                offset += keySize;

                final Value value = cell.getValue();

                if (value.isRemoved()) {
                    fileChannel.write(Bytes.fromLong(-cell.getValue().getTimeStamp()));
                } else {
                    fileChannel.write(Bytes.fromLong(cell.getValue().getTimeStamp()));
                }

                offset += Long.BYTES;
                if (!value.isRemoved()) {
                    final ByteBuffer valueData = value.getData();
                    final int valueSize = value.getData().remaining();
                    fileChannel.write(Bytes.fromInt(valueSize));
                    offset += Integer.BYTES;
                    fileChannel.write(valueData);
                    offset += valueSize;
                }
            }
            for (final Long anOffset : offsets) {
                fileChannel.write(Bytes.fromLong(anOffset));
            }

            fileChannel.write(Bytes.fromLong(offsets.size()));
        }
    }
}
