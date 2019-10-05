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
    private final Table memTable = new MemTable();
    private final long flushThreshold;
    private final File base;
    private int currentGeneration;
    private List<Table> fileTables;

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
        this.flushThreshold = flushThreshold;
        readFiles();
    }

    private void readFiles() throws IOException {
        try (Stream<Path> stream = Files.walk(base.toPath(), 1)
                .filter(path -> {
                    final String fileName = path.getFileName().toString();
                    return fileName.endsWith(SUFFIX) && fileName.contains(TABLE_NAME);
                })) {
            final List<Path> files = stream.collect(Collectors.toList());
            fileTables = new ArrayList<>(files.size());
            currentGeneration = -1;
            files.forEach(path -> {
                final File file = path.toFile();
                try {
                    final FileChannelTable fileChannelTable = new FileChannelTable(file);
                    fileTables.add(fileChannelTable);
                    currentGeneration = Math.max(currentGeneration,
                            FileChannelTable.getGenerationByName(file.getName()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            currentGeneration++;
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> list = new ArrayList<>(fileTables.size() + 1);
        for (final Table fileChannelTable : fileTables) {
            list.add(fileChannelTable.iterator(from));
        }
        final Iterator<Cell> memoryIterator = memTable.iterator(from);
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
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
            if (fileTables.size() > DANGER_COUNT_FILES) {
                mergeTables(0, fileTables.size() / 2);
            }
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value);
        updateData();
    }

    private void flush() throws IOException {
        flush(memTable.iterator(ByteBuffer.allocate(0)), ++currentGeneration, memTable.getBloomFilter());
        memTable.clear();
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
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        updateData();
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        Cell actualCell = memTable.get(key);
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
    private void mergeTables(final int from, final int to) throws IOException {
        final List<Table> mergeFiles = new ArrayList<>(fileTables.subList(from, to));
        final Iterator<Cell> mergeIterator = FileChannelTable.merge(mergeFiles);
        final BitSet mergeBloomFilter = new BitSet();
        for (final Table table : mergeFiles) {
            mergeBloomFilter.or(table.getBloomFilter());
        }

        final List<Table> rightTables = new ArrayList<>(fileTables.subList(to, fileTables.size()));
        fileTables = new ArrayList<>(fileTables.subList(0, from));
        fileTables.addAll(rightTables);

        flush(mergeIterator, ++currentGeneration, mergeBloomFilter);
        for (final Table table : mergeFiles) {
            if (table instanceof FileChannelTable) {
                final FileChannelTable fileTable = (FileChannelTable) table;
                Files.delete(fileTable.getFile().toPath());
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.sizeInBytes() > 0) {
            flush();
        }
    }

    @Override
    public void compact() throws IOException {
        mergeTables(0, fileTables.size());
    }
}
