package ru.mail.polis.persistence;

import com.google.common.collect.Iterators;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class LSMDao implements DAO {
    public static final String SUFFIX_DAT = ".dat";
    private static final String SUFFIX_TMP = ".tmp";
    public static final String PREFIX_FILE = "TABLE";

    private final File file;
    private final MemoryTablePool memTablePool;
    private final NavigableMap<Integer, FileTable> fileTables;

    private static final int TABLES_LIMIT = 10;

//    private ExecutorService executor;

    private Thread flusherThread;

//    private int gen = 0;

    private Log log = LogFactory.getLog(this.getClass());

    private AtomicInteger generationToCompact = new AtomicInteger(0);


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
        AtomicInteger generation = new AtomicInteger(0);
        try (Stream<Path> walk = Files.walk(file.toPath(), 1)) {
            walk.filter(path -> {
                final String filename = path.getFileName().toString();
                return filename.endsWith(SUFFIX_DAT) && filename.startsWith(PREFIX_FILE);
            })
                    .forEach(path -> {
                        try {
                            int currGen = FileTable.fromPath(path);
                            if (currGen >= generation.get()) {
                                generation.set(currGen);
                            }
                            fileTables.put(currGen, new FileTable(path.toFile()));
                        } catch (IOException e) {
                            e.printStackTrace();
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
        Iterator<Cell> memIterator = tableToFlush.getTable().iterator(ByteBuffer.allocate(0));

        if (memIterator.hasNext()) {
            final int generation = tableToFlush.getGeneration();
            final String tempFilename = PREFIX_FILE + generation + SUFFIX_TMP;
            final String filename = PREFIX_FILE + generation + SUFFIX_DAT;

            final File tmp = new File(file, tempFilename);
            FileTable.writeToFile(memIterator, tmp);
            final File dest = new File(file, filename);
            Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
            fileTables.put(generation, new FileTable(dest));
            memTablePool.flushed(generation);

            System.out.println("Flushing generation " + tableToFlush.getGeneration());
        }

        if (fileTables.size() > TABLES_LIMIT) {
            generationToCompact.set(tableToFlush.getGeneration());
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
        FileTable.writeToFile(cellIterator, tmp);
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
                    log.error("Error while flushing {} in generation " + tableToFlush.getGeneration(), e);
                }

            }
            if (!isInterrupted()) {
                System.out.println("Dead after poison");
            }
        }
    }
}
