package ru.mail.polis.service;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.persistence.Bytes;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

public class Node implements Topology<String> {
    private static final Charset CHARSET = Charset.defaultCharset();

    @SuppressWarnings("UnstableApiUsage")
    private final HashFunction hasher;
    @NotNull
    private final String[] nodes;
    @NotNull
    private final String name;

    /**
     * Simple topology implementation.
     *
     * @param set  all names of nodes
     * @param name of current node
     */
    public Node(@NotNull final Set<String> set, @NotNull final String name) {
        assert !set.isEmpty();
        this.name = name;

        nodes = new String[set.size()];
        set.toArray(nodes);
        Arrays.sort(nodes);

        hasher = Hashing.sha256();
    }

    @Override
    public boolean isMe(@NotNull final String topology) {
        return topology.equals(name);
    }

    @Override
    public String me() {
        return name;
    }

    @Override
    public Set<String> primaryFor(final ByteBuffer key, final ReplicationFactor replicationFactor) {
        if (replicationFactor.getFrom() > nodes.length) {
            throw new IllegalArgumentException();
        }

        final String strKey = Arrays.toString(Bytes.toArray(key));

        final PriorityQueue<String> queue = new PriorityQueue<>((node1, node2) -> {
            final long hash1 = myHashCode(strKey, node1);
            final long hash2 = myHashCode(strKey, node2);

            return Long.compare(hash1, hash2);
        });

        queue.addAll(Arrays.asList(nodes));

        final Set<String> res = new HashSet<>(replicationFactor.getFrom() << 1);
        for (int i = 0; i < replicationFactor.getFrom(); i++) {
            res.add(queue.poll());
        }

        return res;
    }

    @Override
    public Set<String> all() {
        return Set.of(nodes);
    }

    private long myHashCode(@NotNull final String key, @NotNull final String node) {
        return hasher.newHasher()
                .putString(node, CHARSET)
                .putString(key, CHARSET)
                .hash().asLong();
    }
}
