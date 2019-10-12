package ru.mail.polis.persistence;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryTablePool implements Table, Closeable {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile MemTable current;
    private NavigableMap<Integer, Table> pendingFlush;
    private BlockingQueue<FlushTable> flushQueue;

    private final long memFlushTrashHold;
    private AtomicBoolean stop = new AtomicBoolean();
    private int generation;
    private final AtomicInteger lastGeneration = new AtomicInteger();


    public MemoryTablePool(final long memFlushTrashHold, final int startGeneration, final int initialQueueCapacity) {
        this.memFlushTrashHold = memFlushTrashHold;
        current = new MemTable();
        pendingFlush = new ConcurrentSkipListMap<>();
        flushQueue = new ArrayBlockingQueue<>(Math.max(2, initialQueueCapacity));
        generation = startGeneration + 1;
        lastGeneration.set(startGeneration);
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            long size = current.sizeInBytes();
            for (final Table table : pendingFlush.values()) {
                size += table.sizeInBytes();
            }
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        lock.readLock().lock();
        final List<Iterator<Cell>> list;

        try {
            list = new ArrayList<>(pendingFlush.size() + 1);
            for (final Table fileChannelTable : pendingFlush.values()) {
                list.add(fileChannelTable.iterator(from));
            }
            final Iterator<Cell> memoryIterator = current.iterator(from);
            list.add(memoryIterator);
        } finally {
            lock.readLock().unlock();
        }


        final Iterator<Cell> iterator = Iters.collapseEquals(Iterators.mergeSorted(list, Cell.COMPARATOR),
                Cell::getKey);

        return Iterators.filter(
                iterator,
                cell -> !cell.getValue().isRemoved());
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped");
        }
        current.upsert(key, value);
        enqueueFlush();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped");
        }
        current.remove(key);
        enqueueFlush();
    }

    @Override
    public void clear() throws IOException {

    }

    @Override
    public Cell get(@NotNull ByteBuffer key) throws IOException {
        return null;
    }

    @Override
    public BitSet getBloomFilter() {
        return null;
    }

    private void enqueueFlush() {
        if (current.sizeInBytes() > memFlushTrashHold) {
            lock.writeLock().lock();
            FlushTable flushTable = null;
            try {
                if (current.sizeInBytes() > memFlushTrashHold) {
                    flushTable = new FlushTable(generation, current, false);
                    generation++;
                    current = new MemTable();
                }
            } finally {
                lock.writeLock().unlock();
            }

            if (flushTable != null) {
                try {
                    flushQueue.put(flushTable);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    protected FlushTable takeTOFlush() throws InterruptedException {
        return flushQueue.take();
    }

    protected void flushed(final int generation) {
        lock.writeLock().lock();
        try {
            pendingFlush.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }

        if (lastGeneration.get() < generation) {
            lastGeneration.set(generation);
        }
    }

    public int getLastGeneration() {
        return lastGeneration.get();
    }

    @Override
    public void close() throws IOException {
        if (!stop.compareAndSet(false, true)) {
            return;
        }


        lock.writeLock().lock();
        final FlushTable flushTable;
        try {
            flushTable = new FlushTable(generation, current, true);
        } finally {
            lock.writeLock().unlock();
        }

        try {
            flushQueue.put(flushTable);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }


    }
}
