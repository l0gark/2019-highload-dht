package ru.mail.polis.persistence;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class MemTable implements Table {
    private final SortedMap<ByteBuffer, Value> map = new ConcurrentSkipListMap<>();
    private AtomicLong sizeInBytes = new AtomicLong(0);
    private final BitSet bloomFilter = new BitSet();

    @Override
    public long sizeInBytes() {
        return sizeInBytes.get();
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                e -> {
                    if (e != null) {
                        return new Cell(e.getKey(), e.getValue());
                    }
                    return null;
                });
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Value previous = map.put(key, Value.of(value));
        if (previous == null) {
            sizeInBytes.addAndGet(key.remaining() + value.remaining());
        } else if (previous.isRemoved()) {
            sizeInBytes.addAndGet(value.remaining());
        } else {
            sizeInBytes.addAndGet(value.remaining() - previous.getData().remaining());
        }
        BloomFilter.setKeyToFilter(bloomFilter, key);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value previous = map.put(key, Value.tombstone());
        if (previous == null) {
            sizeInBytes.addAndGet(key.remaining());
        } else if (!previous.isRemoved()) {
            sizeInBytes.addAndGet(-previous.getData().remaining());
        }
        BloomFilter.setKeyToFilter(bloomFilter, key);
    }

    @Override
    public Cell get(@NotNull final ByteBuffer key) {
        if (!BloomFilter.canContains(bloomFilter, key)) {
            return null;
        }
        final Value value = map.get(key);
        if (value == null) {
            return null;
        }
        return new Cell(key, value);
    }

    @Override
    public BitSet getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public void clear() {
        map.clear();
        bloomFilter.clear();
        sizeInBytes.set(0);
    }
}
