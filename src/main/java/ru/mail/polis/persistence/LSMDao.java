package ru.mail.polis.persistence;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class LSMDao implements DAO {
    static final String SUFFIX_DAT = ".dat";
    static final String SUFFIX_TMP = ".tmp";
    static final String PREFIX_FILE = "TABLE";
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final File file;
    private final MemoryTablePool memTablePool;
    private final NavigableMap<Integer, FileTable> fileTables;

    private static final int TABLES_LIMIT = 10;

    private final Thread flusherThread;

    /**
     * Create persistence DAO.
     * <p>
     * //     * @param file       database location
     * //     * @param flushLimit when we should write to disk
     *
     * @throws IOException if I/O error
     */
    public LSMDao(@NotNull final File file, final long flushLimit, final int queueCapacity) throws IOException {
        assert flushLimit >= 0L;
        this.file = file;
        this.fileTables = new ConcurrentSkipListMap<>();
        final AtomicInteger generation = new AtomicInteger(0);
        try (Stream<Path> walk = Files.walk(file.toPath(), 1)) {
            walk.filter(path -> {
                final String filename = path.getFileName().toString();
                return filename.endsWith(SUFFIX_DAT) && filename.startsWith(PREFIX_FILE);
            })
                    .forEach(path -> {
                        try {
                            final int currentGeneration = FileTable.fromPath(path);
                            if (currentGeneration >= generation.get()) {
                                generation.set(currentGeneration);
                            }
                            fileTables.put(currentGeneration, new FileTable(path.toFile()));
                        } catch (IOException e) {
                            log.error("Something go wrong in reading SSTables, ", e);
                        }
                    });
        }

        memTablePool = new MemoryTablePool(flushLimit, generation.addAndGet(1), queueCapacity);

        flusherThread = new FlusherThread();
        flusherThread.start();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(aliveCells(from), cell -> {
            assert cell != null;
            return Record.of(cell.getKey(), cell.getValue().getData());
        });
    }

    private Iterator<Cell> fileTablesIterator(@NotNull final ByteBuffer from) {
        final List<Iterator<Cell>> iterators = new ArrayList<>();
        for (final FileTable ssTable : this.fileTables.values()) {
            iterators.add(ssTable.iterator(from));
        }

        //noinspection UnstableApiUsage
        return Iters.collapseEquals(
                Iterators.mergeSorted(iterators, Cell.COMPARATOR),
                Cell::getKey
        );
    }

    private Iterator<Cell> aliveCells(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iterators = new ArrayList<>();
        iterators.add(fileTablesIterator(from));
        iterators.add(memTablePool.iterator(from));
        //noinspection UnstableApiUsage
        final Iterator<Cell> cellIterator = Iters.collapseEquals(
                Iterators.mergeSorted(iterators, Cell.COMPARATOR),
                Cell::getKey
        );

        return Iterators.filter(
                cellIterator, cell -> {
                    assert cell != null;
                    return !cell.getValue().isRemoved();
                }
        );
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        memTablePool.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        memTablePool.remove(key);
    }

    private void flush(final FlushTable tableToFlush) throws IOException {
        final Iterator<Cell> memIterator = tableToFlush.getTable().iterator(ByteBuffer.allocate(0));

        if (memIterator.hasNext()) {
            final int generation = tableToFlush.getGeneration();
            final String tempFilename = PREFIX_FILE + generation + SUFFIX_TMP;
            final String filename = PREFIX_FILE + generation + SUFFIX_DAT;

            final File tmp = new File(file, tempFilename);
            Table.write(memIterator, tmp);
            final File dest = new File(file, filename);
            Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
            fileTables.put(generation, new FileTable(dest));
            memTablePool.flushed(generation);
        }

        if (fileTables.size() > TABLES_LIMIT) {
            compact();
        }
    }

    @Override
    public void compact() throws IOException {
        int generation = memTablePool.getLastFlushedGeneration().get();
        System.out.println("Compact generation " + generation + " by thread " + Thread.currentThread().getName());


        final String tempFilename = PREFIX_FILE + generation + SUFFIX_TMP;
        final String filename = PREFIX_FILE + generation + SUFFIX_DAT;

        final Iterator<Cell> cellIterator = fileTablesIterator(ByteBuffer.allocate(0));

        final File tmp = new File(file, tempFilename);
        Table.write(cellIterator, tmp);
        final File dest = new File(file, filename);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);

        fileTables.remove(generation);

        for (final FileTable fileTable : fileTables.values()) {
            Files.delete(fileTable.getFile().toPath());
        }

        fileTables.clear();
        fileTables.put(generation, new FileTable(dest));
        memTablePool.flushed(generation);
    }

    @Override
    public void close() {
        memTablePool.close();
        try {
            flusherThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flusherThread.interrupt();
    }

    private class FlusherThread extends Thread {

        FlusherThread() {
            super("Flusher thread");
        }

        @Override
        public void run() {
            boolean isPoison = false;
            while (!isInterrupted() && !isPoison) {
                FlushTable tableToFlush = null;
                try {
                    tableToFlush = memTablePool.toFlush();
                    isPoison = tableToFlush.isPoisonPill();
                    flush(tableToFlush);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    log.error("Error while flushing {} in generation ", tableToFlush.getGeneration(), e);
                }

            }
            if (!isInterrupted()) {
                System.out.println("Dead after poison");
            }
        }
    }
}
