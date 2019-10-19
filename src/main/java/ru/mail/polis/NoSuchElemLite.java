package ru.mail.polis;

import java.util.NoSuchElementException;

public class NoSuchElemLite extends NoSuchElementException {
    private static final long serialVersionUID = 1278761245283473322L;

    public NoSuchElemLite(final String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
