package ru.mail.polis.service;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.persistence.Bytes;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
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
     * @param set all names of nodes
     * @param name of current node
     */
    public Node(@NotNull final Set<String> set, @NotNull final String name) {
        assert !set.isEmpty();
        this.name = name;

        nodes = new String[set.size()];
        set.toArray(nodes);
        Arrays.sort(nodes);
        //noinspection UnstableApiUsage
        hasher = Hashing.sha256();
    }

    @Override
    public boolean isMe(@NotNull final String topology) {
        return name.equals(topology);
    }

    @Override
    public String primaryFor(final ByteBuffer key) {
        final String strKey = Arrays.toString(Bytes.toArray(key));
        String minNode = nodes[0];

        for (int i = 1; i < nodes.length; i++) {
            final String current = nodes[i];

            final long minHash = myHashCode(strKey, minNode);
            final long curHash = myHashCode(strKey, current);

            if (minHash > curHash) {
                minNode = current;
            }
        }

        return minNode;
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
