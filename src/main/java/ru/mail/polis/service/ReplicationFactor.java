package ru.mail.polis.service;

public class ReplicationFactor {

    private final int ack;
    private final int from;

    /**
     * Replication factor of value.
     *
     * @param ack necessary
     * @param from count
     * @return replication factor
     */
    public ReplicationFactor(final int ack, final int from) {
        if (ack < 1 || ack > from) {
            throw new IllegalArgumentException("kmfkefmek");
        }

        this.ack = ack;
        this.from = from;
    }

    /**
     * Parse fromString.
     *
     * @param replica String format
     * @return
     * @throws IllegalArgumentException when ack < 1 || ack > from
     */
    public static ReplicationFactor fromString(final String replica) throws IllegalArgumentException {
        final int index = replica.indexOf('/');
        if (index < 0 || index != replica.lastIndexOf('/')) {
            throw new IllegalArgumentException("LOL");
        }

        final int ack = Integer.parseInt(replica.substring(0, index));
        final int from = Integer.parseInt(replica.substring(index + 1));

        return new ReplicationFactor(ack, from);
    }

    /**
     * Necessary count of nodes.
     *
     * @param nodes count
     * @return ReplicationFactor
     */
    public static ReplicationFactor quorum(final int nodes) {
        return new ReplicationFactor(nodes / 2 + 1, nodes);
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    @Override
    public String toString() {
        return ack + "/" + from;
    }
}
