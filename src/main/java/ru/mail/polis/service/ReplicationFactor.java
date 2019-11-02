package ru.mail.polis.service;

public class ReplicationFactor {

    private final int ack;
    private final int from;

    /**
     * @param ack  neccessary
     * @param from optional
     */
    public ReplicationFactor(int ack, int from) {
        if (ack < 1 || ack > from) {
            throw new IllegalArgumentException("kmfkefmek");
        }

        this.ack = ack;
        this.from = from;
    }

    public static ReplicationFactor fromString(final String replica) throws IllegalArgumentException {
        int index = replica.indexOf('/');
        if (index < 0 || index != replica.lastIndexOf('/')) {
            throw new IllegalArgumentException("LOL");
        }

        int ack = Integer.parseInt(replica.substring(0, index));
        int from = Integer.parseInt(replica.substring(index + 1));

        return new ReplicationFactor(ack, from);
    }

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
