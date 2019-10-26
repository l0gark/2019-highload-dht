package ru.mail.polis.service;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Set;

@ThreadSafe
public interface Topology<T> {

    boolean isMe(T topology);

    T primaryFor(ByteBuffer key);

    Set<T> all();
}
