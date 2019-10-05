package ru.mail.polis;

import java.util.NoSuchElementException;

public class NoSuchElemLite extends NoSuchElementException {
    public NoSuchElemLite(String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
