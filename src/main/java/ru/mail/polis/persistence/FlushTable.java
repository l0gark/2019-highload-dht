package ru.mail.polis.persistence;

public class FlushTable {
    private final int generation;
    private final Table table;
    private final boolean poisonPill;

    public FlushTable(int generation, Table table, boolean poisonPill) {
        this.generation = generation;
        this.table = table;
        this.poisonPill = poisonPill;
    }

    public FlushTable(int generation, Table table) {
        this(generation, table, false);
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
