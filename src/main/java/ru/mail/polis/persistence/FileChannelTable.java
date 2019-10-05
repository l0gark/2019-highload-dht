package ru.mail.polis.persistence;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class FileChannelTable implements Table {
    private static final String UNSUPPORTED_EXCEPTION_MESSAGE = "FileTable has not access to update!";
    private final int rows;
    private final long beginOffsets;
    private final File file;
    private final BitSet bloomFilter;

    /**
     * Sorted String Table, which use FileChannel for Read_and_Write operations.
     *
     * @param file of this table
     * @throws IOException If an I/O error occurs
     */
    public FileChannelTable(final File file) throws IOException {
        this.file = file;
        try (FileChannel fc = openReadFileChannel()) {
            assert fc != null;

            // Rows
            long offset = fc.size() - Long.BYTES;
            final long rowsValue = readLong(fc, offset);
            assert rowsValue <= Integer.MAX_VALUE;
            this.rows = (int) rowsValue;

            // BloomFilter
            offset -= Integer.BYTES;
            final int bloomFilterSize = readInt(fc, offset);
            offset -= (long) bloomFilterSize * Long.BYTES;
            final ByteBuffer bloomFilterBuffer = readBuffer(fc, offset, bloomFilterSize * Long.BYTES);
            final long[] bloomFilterArray = new long[bloomFilterSize];
            for (int i = 0, bloomFilterOffset = 0; i < bloomFilterSize; i++, bloomFilterOffset += Long.BYTES) {
                bloomFilterArray[i] = bloomFilterBuffer.getLong(bloomFilterOffset);
            }
            bloomFilter = BitSet.valueOf(bloomFilterArray);

            // begin offset
            this.beginOffsets = offset - (long) Long.BYTES * rows;
        }
    }

    /**
     * Merge list of SSTables.
     *
     * @param tables list of SSTables
     * @return MergedIterator with latest versions of key-value
     */
    public static Iterator<Cell> merge(@NotNull final List<Table> tables) {
        final List<Iterator<Cell>> list = new ArrayList<>(tables.size());
        for (final Table table : tables) {
            try {
                list.add(table.iterator(ByteBuffer.allocate(0)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return Iters.collapseEquals(Iterators.mergeSorted(list, Cell.COMPARATOR), Cell::getKey);
    }

    public File getFile() {
        return file;
    }

    private FileChannel openReadFileChannel() {
        try {
            return FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int readInt(final FileChannel fc, final long offset) {
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        try {
            fc.read(buffer, offset);
            return buffer.rewind().getInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private long readLong(final FileChannel fc, final long offset) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        try {
            fc.read(buffer, offset);
            return buffer.rewind().getLong();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1L;
    }

    private ByteBuffer readBuffer(final FileChannel fc, final long offset, final int size) {
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        try {
            fc.read(buffer, offset);
            return buffer.rewind();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer;
    }

    private long getOffset(final FileChannel fc, final int i) {
        final ByteBuffer offsetBB = ByteBuffer.allocate(Long.BYTES);
        try {
            fc.read(offsetBB, beginOffsets + (long) Long.BYTES * i);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return offsetBB.rewind().getLong();
    }

    @NotNull
    private ByteBuffer keyAt(final int i) {
        assert 0 <= i && i < rows;
        try (FileChannel fc = openReadFileChannel()) {
            assert fc != null;
            long offset = getOffset(fc, i);
            assert offset <= Integer.MAX_VALUE;

            // KeySize
            final int keySize = readInt(fc, offset);
            offset += Integer.BYTES;
            // Key
            return readBuffer(fc, offset, keySize);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Cell cellAt(final int i) {
        assert 0 <= i && i < rows;
        try (FileChannel fc = openReadFileChannel()) {
            assert fc != null;
            long offset = getOffset(fc, i);
            assert offset <= Integer.MAX_VALUE;

            //KeySize
            final int keySize = readInt(fc, offset);
            offset += Integer.BYTES;

            //Key
            final ByteBuffer key = readBuffer(fc, offset, keySize);
            offset += keySize;

            //Timestamp
            final long timeStamp = readLong(fc, offset);
            offset += Long.BYTES;

            if (timeStamp < 0) {
                return new Cell(key, Value.tombstone(-timeStamp));
            }
            //valueSize
            final int valueSize = readInt(fc, offset);
            offset += Integer.BYTES;

            //value
            final ByteBuffer value = readBuffer(fc, offset, valueSize);
            return new Cell(key, Value.of(timeStamp, value));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int position(final ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + ((right - left) >> 1);
            final int cmp = from.compareTo(keyAt(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    static int getGenerationByName(final String name) {
        for (int index = 0; index < Math.min(9, name.length()); index++) {
            if (!Character.isDigit(name.charAt(index))) {
                return index == 0 ? 0 : Integer.parseInt(name.substring(0, index));
            }
        }
        return -1;
    }

    @Override
    public long sizeInBytes() {
        return 0;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            int next = position(from);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Iterator is empty");
                }
                return cellAt(next++);
            }
        };
    }

    @Override
    public Cell get(@NotNull final ByteBuffer key) {
        if (!BloomFilter.canContains(bloomFilter, key)) {
            return null;
        }
        final int position = position(key);
        if (position < 0 || position >= rows) {
            return null;
        }
        final Cell cell = cellAt(position);
        if (!cell.getKey().equals(key)) {
            return null;
        }
        return cell;
    }


    @Override
    public BitSet getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException(UNSUPPORTED_EXCEPTION_MESSAGE);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException(UNSUPPORTED_EXCEPTION_MESSAGE);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(UNSUPPORTED_EXCEPTION_MESSAGE);
    }
}
