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
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.persistence.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static ru.mail.polis.service.LocalClient.sendResponse;

public class SimpleServer extends HttpServer implements Service {
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
        final boolean proxied = ResponseUtils.isProxied(request);

        if (proxied) {
            getLocal(request, session, key);
            return;
        }
        getFromSet(request, session, replicas, key);
    }

    private void getLocal(@NotNull final Request request,
                          @NotNull final HttpSession session,
                          @NotNull final ByteBuffer key) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                executeAsync(session, () -> LocalClient.getMethod(dao, key));
                break;
            case Request.METHOD_PUT:
                executeAsync(session, () -> LocalClient.putMethod(dao, key, request));
                break;
            case Request.METHOD_DELETE:
                executeAsync(session, () -> {
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                });
                break;
            default:
                sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
                break;
        }
    }

    private void getFromSet(@NotNull final Request request,
                            @NotNull final HttpSession session,
                            @Nullable final String replicas,
                            @NotNull final ByteBuffer key) {
        ReplicationFactor replicationFactor;
        try {
            replicationFactor = replicas == null ? quorum : ReplicationFactor.fromString(replicas);
        } catch (IllegalArgumentException e) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final Set<String> nodes = topology.primaryFor(key, replicationFactor);

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                scheduleGetEntity(session, request, key, replicationFactor, nodes);
                break;
            case Request.METHOD_PUT:
                schedulePutEntity(session, request, key, replicationFactor, nodes);
                break;
            case Request.METHOD_DELETE:
                scheduleDeleteEntity(session, request, key, replicationFactor, nodes);
                break;
            default:
                sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
        }
    }

    private void scheduleGetEntity(
            @NotNull final HttpSession session,
            @NotNull final Request request,
            @NotNull final ByteBuffer key,
            @NotNull final ReplicationFactor replicationFactor,
            @NotNull final Set<String> nodes) {

        CompletableFuture.supplyAsync(() -> {
            final Queue<Value> queue = new ConcurrentLinkedQueue<>();
            for (final String node : nodes) {
                Response response = null;
                if (topology.isMe(node)) {
                    try {
                        response = LocalClient.getMethod(dao, key);
                    } catch (IOException e) {
                        log.error("Can`t read from drive", e);
                    }
                } else {
                    response = proxy(node, request);
                }

                if (response != null && response.getStatus() != 400) {
                    queue.add(ResponseUtils.responseToValue(response));
                }
            }
            return queue;
        }, executor).thenAccept(queue -> {
            if (queue.size() < replicationFactor.getAck()) {
                sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                return;
            }
            final Value value = Value.merge(queue);
            sendResponse(session, ResponseUtils.valueToResponse(value));
        }).exceptionally(error -> {
            log.error("Error in scheduleGet", error);
            return null;
        });
    }

    private void schedulePutEntity(
            @NotNull final HttpSession session,
            @NotNull final Request request,
            @NotNull final ByteBuffer key,
            @NotNull final ReplicationFactor replicationFactor,
            @NotNull final Set<String> nodes) {
        final AtomicInteger count = new AtomicInteger(0);
        CompletableFuture.runAsync(() -> {
            for (final String node : nodes) {
                if (ResponseUtils.is2XX(proxy(node, request))) {
                    count.incrementAndGet();
                } else if (topology.isMe(node)) {
                    try {
                        final Response response = LocalClient.putMethod(dao, key, request);
                        if (ResponseUtils.is2XX(response)) {
                            count.incrementAndGet();
                        }
                    } catch (IOException e) {
                        log.error(":(", e);
                    }
                }
            }
        }, executor).thenAccept(v -> {
            if (count.get() < replicationFactor.getAck()) {
                sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                return;
            }
            sendResponse(session, new Response(Response.CREATED, Response.EMPTY));
        }).exceptionally(error -> {
            log.error("Error in schedulePut", error);
            return null;
        });
    }

    private void scheduleDeleteEntity(@NotNull final HttpSession session,
                                      @NotNull final Request request,
                                      @NotNull final ByteBuffer key,
                                      @NotNull final ReplicationFactor replicationFactor,
                                      @NotNull final Set<String> nodes) {
        final AtomicInteger count = new AtomicInteger(0);
        CompletableFuture.runAsync(() -> {
            for (final String node : nodes) {
                if (ResponseUtils.is2XX(proxy(node, request))) {
                    count.incrementAndGet();
                } else if (topology.isMe(node)) {
                    try {
                        final Response response = LocalClient.deleteMethod(dao, key);
                        if (ResponseUtils.is2XX(response)) {
                            count.incrementAndGet();
                        }
                    } catch (IOException e) {
                        log.error("Error was thrown while read from drive", e);
                    }
                }
            }
        }, executor).thenAccept(v -> {
            if (count.get() < replicationFactor.getAck()) {
                sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                return;
            }
            sendResponse(session, new Response(Response.ACCEPTED, Response.EMPTY));
        }).exceptionally(error -> {
            log.error("Error in scheduleDelete", error);
            return null;
        });
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
    public void handleDefault(final Request request, final HttpSession session) {
        sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
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

    private Response proxy(@NotNull final String workerNode, @NotNull final Request request) {
        try {
            request.addHeader(ResponseUtils.HEADER_PROXY);
            final HttpClient client = pool.get(workerNode);
            if (client == null) {
                return new Response(Response.BAD_REQUEST, Response.EMPTY);
            }
            return client.invoke(request);
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
