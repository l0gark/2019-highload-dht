package ru.mail.polis.service;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

/**
 * HTTP client for {@link ru.mail.polis.service.Service}.
 */
public interface ServiceClient {
    /**
     * Get a value by key.
     *
     * @param key key
     * @return HTTP response
     */
    @NotNull
    Future<Response> get(@NotNull ByteBuffer key);

    /**
     * Put a value by key.
     *
     * @param key   key
     * @param request value
     * @return HTTP response
     */
    @NotNull
    Future<Response> put(
            @NotNull ByteBuffer key,
            @NotNull Request request);

    /**
     * Delete a value by key.
     *
     * @param key key
     * @return HTTP response
     */
    @NotNull
    Future<Response> delete(@NotNull ByteBuffer key);
}
