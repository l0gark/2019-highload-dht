package ru.mail.polis.persistence;

public class FlushTable {
    private final int generation;
    private final Table table;
    private final boolean poisonPill;

    /**
     * Table to Flush.
     *
     * @param table to flush
     * @param generation of table
     * @param poisonPill is death for dao?
     */
    public FlushTable(final Table table, final int generation, final boolean poisonPill) {
        this.generation = generation;
        this.table = table;
        this.poisonPill = poisonPill;
    }

    /**
     * Standart constructor without poisonPill.
     *
     * @param table of flush
     * @param generation is death for dao?
     */
    public FlushTable(final Table table, final int generation) {
        this(table, generation, false);
    }

    public int getGeneration() {
        return generation;
    }

    public Table getTable() {
        return table;
    }

    public boolean isPoisonPill() {
        return poisonPill;
    }
}
