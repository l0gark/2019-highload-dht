package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.NoSuchElementException;

import java.util.concurrent.Executor;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SimpleServer extends HttpServer implements Service {

    private static final Logger log = LoggerFactory.getLogger(SimpleServer.class);
    private final DAO dao;
    private final Executor executor;

    private final Topology<String> topology;
    private final Map<String, HttpClient> pool;

    /**
     * Simple implementation of Service.
     *
     * @param port     to listen
     * @param dao      implementation
     * @param executor pool
     * @throws IOException io
     */
    public SimpleServer(final int port,
                        @NotNull final DAO dao,
                        @NotNull final Executor executor,
                        @NotNull final Topology<String> topology) throws IOException {
        super(getConfig(port));
        this.dao = dao;
        this.executor = executor;
        this.topology = topology;

        final Set<String> nodes = topology.all();
        pool = new HashMap<>(nodes.size() << 1);
        for (final String name : nodes) {
            if (!this.topology.isMe(name)) {
                pool.put(name, new HttpClient(new ConnectionString(name + "?timeout=100")));
            }
        }

        log.info("Server is running on port " + port);
    }

    /**
     * Method for get status.
     *
     * @return state
     */
    @Path("/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    /**
     * Http interface for dao.
     *
     * @param request http request
     * @param id      key
     * @param session session
     */
    @Path("/v0/entity")
    public void daoMethods(@NotNull final Request request,
                           @Param("id") final String id,
                           final HttpSession session) {
        if (id == null || id.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));

        final String workerNode = topology.primaryFor(key);

        if (!topology.isMe(workerNode)) {
            executeAsync(session, () -> proxy(workerNode, request));
            return;
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                executeAsync(session, () -> getMethod(key));
                break;
            case Request.METHOD_PUT:
                executeAsync(session, () -> putMethod(key, request));
                break;
            case Request.METHOD_DELETE:
                executeAsync(session, () -> {
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                });
                break;
            default:
                sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
        }
    }

    /**
     * Range method.
     *
     * @param request http request
     * @param session session
     * @param start   of range
     * @param end     of range
     */
    @Path("/v0/entities")
    public void entities(final Request request, final HttpSession session, @Param("start") final String start,
                         @Param("end") final String end) {
        if (start == null || start.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        try {
            final Iterator<Record> records = dao.range(ByteBuffer.wrap(start.getBytes(UTF_8)),
                    end == null || end.isEmpty() ? null : ByteBuffer.wrap(end.getBytes(UTF_8)));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            log.error("Entities sending exception", e);
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65536) {
            throw new IllegalArgumentException("Invalid port");
        }
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        config.selectors = 4;
        return config;
    }

    private void executeAsync(@NotNull final HttpSession session, @NotNull final Action action) {
        executor.execute(() -> {
            try {
                sendResponse(session, action.act());
            } catch (NoSuchElementException e) {
                sendResponse(session, new Response(Response.NOT_FOUND, Response.EMPTY));
            } catch (IOException e) {
                log.error("Execute exception", e);
            }
        });
    }

    private Response getMethod(final ByteBuffer key) throws IOException {
        final ByteBuffer value = dao.get(key);
        final ByteBuffer duplicate = value.duplicate();
        final byte[] body = new byte[duplicate.remaining()];
        duplicate.get(body);
        return new Response(Response.OK, body);
    }

    private Response putMethod(final ByteBuffer key, final Request request) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private static void sendResponse(@NotNull final HttpSession session,
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

    private Response proxy(@NotNull final String workerNode, @NotNull final Request request) {
        try {
            return pool.get(workerNode).invoke(request);
        } catch (InterruptedException | PoolException | HttpException | IOException e) {
            log.error("Request proxy error ", e);
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }
}
