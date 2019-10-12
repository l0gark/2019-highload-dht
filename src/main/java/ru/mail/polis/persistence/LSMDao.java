package ru.mail.polis.persistence;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LSMDao implements DAO {
    private static final String TABLE_NAME = "SSTable";
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";
    private static final int DANGER_COUNT_FILES = 5;
    private final MemoryTablePool tablePool;
    private final File base;
    private List<Table> fileTables;
    private final FlusherThread flusher;

    private class FlusherThread extends Thread {
        public FlusherThread() {
            super("flusher");
        }

        @Override
        public void run() {
            boolean poisonReceived = false;
            while (!isInterrupted() && !poisonReceived) {
                FlushTable flushTable = null;
                try {
                    flushTable = tablePool.takeTOFlush();
                    poisonReceived = flushTable.isPoisonPill();
                    flush(flushTable.getGeneration(), flushTable.getTable());
                    tablePool.flushed(flushTable.getGeneration());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * NoSql Dao.
     *
     * @param base           directory of DB
     * @param flushThreshold maxsize of @memTable
     * @throws IOException If an I/O error occurs
     */
    public LSMDao(final File base, final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        int startGeneration = readFiles();
        tablePool = new MemoryTablePool(flushThreshold, startGeneration, 2);
        flusher = new FlusherThread();
        flusher.start();
    }

    /**
     * @return currentGeneration
     * @throws IOException
     */
    private int readFiles() throws IOException {
        try (Stream<Path> stream = Files.walk(base.toPath(), 1)
                .filter(path -> {
                    final String fileName = path.getFileName().toString();
                    return fileName.endsWith(SUFFIX) && fileName.contains(TABLE_NAME);
                })) {
            final List<Path> files = stream.collect(Collectors.toList());
            fileTables = new ArrayList<>(files.size());
            int currentGeneration = -1;
            for (Path path : files) {
                final File file = path.toFile();
                try {
                    final FileChannelTable fileChannelTable = new FileChannelTable(file);
                    fileTables.add(fileChannelTable);
                    currentGeneration = Math.max(currentGeneration,
                            FileChannelTable.getGenerationByName(file.getName()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return ++currentGeneration;
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> list = new ArrayList<>(fileTables.size() + 1);
        for (final Table fileChannelTable : fileTables) {
            list.add(fileChannelTable.iterator(from));
        }
        final Iterator<Cell> memoryIterator = tablePool.iterator(from);
        list.add(memoryIterator);
        final Iterator<Cell> iterator = Iters.collapseEquals(Iterators.mergeSorted(list, Cell.COMPARATOR),
                Cell::getKey);

        final Iterator<Cell> alive =
                Iterators.filter(
                        iterator,
                        cell -> !cell.getValue().isRemoved());

        return Iterators.transform(
                alive,
                cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    private void updateData() throws IOException {
        if (fileTables.size() > DANGER_COUNT_FILES) {
            mergeTables(0, fileTables.size() >> 1, tablePool.getLastGeneration());
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        tablePool.upsert(key.duplicate(), value);
    }

    private void flush(final int currentGeneration, final Table table) throws IOException {
        flush(tablePool.iterator(ByteBuffer.allocate(0)), currentGeneration, table.getBloomFilter());
        tablePool.clear();
    }

    private void flush(final Iterator<Cell> iterator,
                       final int generation,
                       final BitSet bloomFilter)
            throws IOException {
        final File tmp = new File(base, generation + TABLE_NAME + TEMP);
        Table.write(iterator, tmp, bloomFilter);
        final File dest = new File(base, generation + TABLE_NAME + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        fileTables.add(new FileChannelTable(dest));
        updateData();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        tablePool.remove(key);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        Cell actualCell = tablePool.get(key);
        for (final Table table : fileTables) {
            final Cell cell = table.get(key);
            if (cell == null) {
                continue;
            }
            if (actualCell == null || Cell.COMPARATOR.compare(cell, actualCell) < 0) {
                actualCell = cell;
            }
        }
        if (actualCell == null || actualCell.getValue().isRemoved()) {
            throw new NoSuchElementException("");
        }
        final Record record = Record.of(actualCell.getKey(), actualCell.getValue().getData());
        return record.getValue();
    }

    /**
     * merge tables.
     *
     * @param from inclusive
     * @param to   exclusive
     * @throws IOException If an I/O error occurs
     */
    private void mergeTables(final int from, final int to, final int currentGeneration) throws IOException {
        final List<Table> mergeFiles = new ArrayList<>(fileTables.subList(from, to));
        final Iterator<Cell> mergeIterator = FileChannelTable.merge(mergeFiles);
        final BitSet mergeBloomFilter = new BitSet();
        for (final Table table : mergeFiles) {
            mergeBloomFilter.or(table.getBloomFilter());
        }

        final List<Table> rightTables = new ArrayList<>(fileTables.subList(to, fileTables.size()));
        fileTables = new ArrayList<>(fileTables.subList(0, from));
        fileTables.addAll(rightTables);

        flush(mergeIterator, currentGeneration, mergeBloomFilter);
        for (final Table table : mergeFiles) {
            if (table instanceof FileChannelTable) {
                final FileChannelTable fileTable = (FileChannelTable) table;
                Files.delete(fileTable.getFile().toPath());
            }
        }
    }

    @Override
    public void close() throws IOException {
        tablePool.close();
        try {
            flusher.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void compact() throws IOException {
        mergeTables(0, fileTables.size(), tablePool.getLastGeneration() + 1);
    }
}
