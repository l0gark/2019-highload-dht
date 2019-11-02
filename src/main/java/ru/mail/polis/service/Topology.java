package ru.mail.polis.service;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Set;

@ThreadSafe
public interface Topology<T> {

    boolean isMe(T topology);

    Set<T> primaryFor(ByteBuffer key, ReplicationFactor replicationFactor);

    Set<T> all();
}
