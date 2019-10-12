package ru.mail.polis.persistence;

public class FlushTable {
    private final int generation;
    private final Table table;
    private final boolean poisonPill;

    public FlushTable(Table table, int generation, boolean poisonPill) {
        this.generation = generation;
        this.table = table;
        this.poisonPill = poisonPill;
    }

    public FlushTable(Table table, int generation) {
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
