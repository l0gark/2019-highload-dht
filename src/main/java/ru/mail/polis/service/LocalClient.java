package ru.mail.polis.service;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.NoSuchElemLite;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.persistence.Value;

import java.io.IOException;
import java.nio.ByteBuffer;

final class LocalClient {
    private static final Logger log = LoggerFactory.getLogger(LocalClient.class);

    private LocalClient() {
    }

    static Response getMethod(final DAO dao, final ByteBuffer key) {
        final Value value;
        try {
            value = dao.getValue(key);
        } catch (NoSuchElemLite e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            log.error("Error while put to dao", e);
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        if (value == null) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return ResponseUtils.valueToResponse(value);
    }

    static Response putMethod(final DAO dao, final ByteBuffer key, final Request request) {
        try {
            dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        } catch (IOException e) {
            log.error("Error while put to dao", e);
            return new Response(Response.BAD_REQUEST, Response.EMPTY);

        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @NotNull
    static Response deleteMethod(final DAO dao, final ByteBuffer key) {
        try {
            dao.remove(key);
        } catch (IOException e) {
            log.error("Error while delete from dao", e);
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        return new Response(Response.OK, Response.EMPTY);
    }

    static void sendResponse(@NotNull final HttpSession session,
                             @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, "Error while send response");
            } catch (IOException ex) {
                log.error("Error while send error");
            }
        }
    }

    static void sendResponse(@NotNull final HttpSession session,
                             @NotNull final String status) {
        sendResponse(session, new Response(status, Response.EMPTY));
    }
}
