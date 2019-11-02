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
import ru.mail.polis.NoSuchElemLite;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.persistence.Bytes;
import ru.mail.polis.persistence.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import java.util.concurrent.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SimpleServer extends HttpServer implements Service {
    static final String HEADER_PROXY = "X-OK-Proxy: True";

    private static final Logger log = LoggerFactory.getLogger(SimpleServer.class);
    private final DAO dao;
    private final Executor executor;

    private final Topology<String> topology;
    private final Map<String, HttpClient> pool;

    private final ReplicationFactor quorum;

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
        this.pool = new HashMap<>(nodes.size() << 1);
        for (final String name : nodes) {
            if (!this.topology.isMe(name)) {
                this.pool.put(name, new HttpClient(new ConnectionString(name + "?timeout=100")));
            }
        }

        this.quorum = ReplicationFactor.quorum(nodes.size());

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
                           @Param("replicas") final String replicas,
                           final HttpSession session) {
        if (id == null || id.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }


        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        final boolean proxied = isProxied(request);

        if (proxied) {
            getLocal(request, session, key);
            return;
        }
        getFromSet(request, session, replicas, key);
    }

    private void getLocal(Request request, HttpSession session, ByteBuffer key) {
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

    private Value responseToValue(Response response) {
        final var ts = response.getHeader("TIME_STAMP: ");
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

    private void getFromSet(Request request, HttpSession session, String replicas, ByteBuffer key) {
        ReplicationFactor replicationFactor;
        try {
            replicationFactor = replicas == null ? quorum : ReplicationFactor.fromString(replicas);
        } catch (IllegalArgumentException e) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final Set<String> nodes = topology.primaryFor(key, replicationFactor);

        executeAsync(session, () -> {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    List<Value> values = new ArrayList<>(nodes.size());
                    for (final String node : nodes) {
                        Response response;
                        if (topology.isMe(node)) {
                            response = getMethod(key);
                        } else {
                            response = proxy(node, request);
                        }
                        if(response.getStatus() != 400){
                            values.add(responseToValue(response));
                        }
                    }

                    if (values.size() < replicationFactor.getAck()) {
                        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                    }
                    Value value = Value.merge(values);
                    return valueToResponse(value);
                case Request.METHOD_PUT:
                    int count = 0;
                    for (final String node : nodes) {
                        if (topology.isMe(node)) {
                            if (is2XX(putMethod(key, request).getStatus())) {
                                count++;
                            }
                        }
                        if (is2XX(proxy(node, request).getStatus())) {
                            count++;
                        }
                    }

                    if (count < replicationFactor.getAck()) {
                        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                    }

                    return new Response(Response.CREATED, Response.EMPTY);
                case Request.METHOD_DELETE:
                    count = 0;
                    for (final String node : nodes) {
                        if (topology.isMe(node)) {
                            if (is2XX(deleteMethod(key).getStatus())) {
                                count++;
                            }
                        }
                        if (is2XX(proxy(node, request).getStatus())) {
                            count++;
                        }
                    }

                    if (count < replicationFactor.getAck()) {
                        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                    }
                    return new Response(Response.ACCEPTED, Response.EMPTY);

            }
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        });
    }

    private boolean is2XX(final int code) {
        return code <= 299 && code >= 200;
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

    private Response valueToResponse(final Value value) {
        if (value.state() == Value.State.PRESENT) {
            final var response = Response.ok(Bytes.toArray(value.getData()));
            response.addHeader("TIME_STAMP: " + value.getTimeStamp());
            return response;
        } else if (value.state() == Value.State.REMOVED) {
            final var response = new Response(Response.NOT_FOUND, Response.EMPTY);
            response.addHeader("TIME_STAMP: " + value.getTimeStamp());
            return response;
        }
        return new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    private Response getMethod(final ByteBuffer key) throws IOException {
        final Value value;
        try {
            value = dao.getValue(key);
        } catch (NoSuchElemLite e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        if (value == null) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return valueToResponse(value);
    }

    private Response putMethod(final ByteBuffer key, final Request request) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response deleteMethod(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.OK, Response.EMPTY);
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
            request.addHeader(HEADER_PROXY);
            return pool.get(workerNode).invoke(request);
        } catch (InterruptedException | PoolException | HttpException | IOException | NullPointerException e) {
            log.error("Request proxy error ", e);
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
    }

    static boolean isProxied(@NotNull final Request request) {
        return request.getHeader(HEADER_PROXY) != null;
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

}
