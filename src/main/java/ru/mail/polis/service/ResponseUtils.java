package ru.mail.polis.service;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.persistence.Bytes;
import ru.mail.polis.persistence.Value;

import java.nio.ByteBuffer;

final class ResponseUtils {
    static final String HEADER_PROXY = "X-OK-Proxy: True";
    private static final String HEADER_TIME_STAMP = "TIME_STAMP: ";

    private ResponseUtils() {
    }

    static boolean is2XX(final int code) {
        return code <= 299 && code >= 200;
    }

    static boolean isProxied(@NotNull final Request request) {
        return request.getHeader(HEADER_PROXY) != null;
    }

    static Response valueToResponse(final Value value) {
        if (value.state() == Value.State.PRESENT) {
            final var response = Response.ok(Bytes.toArray(value.getData()));
            response.addHeader(HEADER_TIME_STAMP + value.getTimeStamp());
            return response;
        } else if (value.state() == Value.State.REMOVED) {
            final var response = new Response(Response.NOT_FOUND, Response.EMPTY);
            response.addHeader(HEADER_TIME_STAMP + value.getTimeStamp());
            return response;
        }
        return new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    static Value responseToValue(final Response response) {
        final String ts = response.getHeader(ResponseUtils.HEADER_TIME_STAMP);
        if (response.getStatus() == 200) {
            if (ts == null) {
                throw new IllegalArgumentException();
            }
            return Value.of(Long.parseLong(ts), ByteBuffer.wrap(response.getBody()));
        } else {
            if (ts == null) {
                return Value.absent();
            }
            return Value.tombstone(Long.parseLong(ts));
        }
    }
}
