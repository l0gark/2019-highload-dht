package ru.mail.polis.service;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.persistence.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.mail.polis.service.LocalClient.sendResponse;

final class ProxyHelper {
    private static final Logger log = LoggerFactory.getLogger(ProxyHelper.class);

    private final Topology<String> topology;
    private final DAO dao;
    private final Executor executor;
    private final Map<String, HttpClient> pool;

    ProxyHelper(@NotNull final Topology<String> topology,
                @NotNull final DAO dao,
                @NotNull final Executor executor,
                @NotNull final Map<String, HttpClient> pool) {
        this.topology = topology;
        this.dao = dao;
        this.executor = executor;
        this.pool = pool;
    }

    void scheduleGetEntity(
            @NotNull final HttpSession session,
            @NotNull final RequestData data) {

        final Queue<Value> queue = new ConcurrentLinkedQueue<>();
        final AtomicInteger doneCount = new AtomicInteger(0);
        final AtomicBoolean sessionSent = new AtomicBoolean(false);

        for (final String node : data.nodes) {
            CompletableFuture.runAsync(() -> {
                Response response;
                if (topology.isMe(node)) {
                    response = LocalClient.getMethod(dao, data.key);
                } else {
                    response = proxy(node, data.request);
                }

                if (response != null && response.getStatus() != 400) {
                    queue.add(ResponseUtils.responseToValue(response));
                }

                doneCount.incrementAndGet();
            }, executor).thenAccept(v -> {
                if (!sessionSent.get()) {
                    if (queue.size() >= data.rf.getAck()) {
                        final Value value = Value.merge(queue);
                        sendResponse(session, ResponseUtils.valueToResponse(value));
                        sessionSent.set(true);
                    } else if (doneCount.get() - queue.size() > data.rf.getFrom() - data.rf.getAck()) {
                        sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                        sessionSent.set(true);
                    }
                }
            }).exceptionally(e -> {
                log.error("Error in scheduleGet", e);
                return null;
            });
        }
    }

    void schedulePutEntity(
            @NotNull final HttpSession session,
            @NotNull final RequestData data) {
        final AtomicInteger goodCount = new AtomicInteger(0);
        final AtomicInteger doneCount = new AtomicInteger(0);
        final AtomicBoolean sessionSent = new AtomicBoolean(false);

        for (final String node : data.nodes) {
            CompletableFuture.runAsync(() -> {
                if (ResponseUtils.is2XX(proxy(node, data.request))) {
                    goodCount.incrementAndGet();
                } else if (topology.isMe(node)) {
                    final Response response = LocalClient.putMethod(dao, data.key, data.request);
                    if (ResponseUtils.is2XX(response)) {
                        goodCount.incrementAndGet();
                    }
                }
                doneCount.incrementAndGet();
            }, executor).thenAccept(v -> {
                if (!sessionSent.get()) {
                    if (goodCount.get() >= data.rf.getAck()) {
                        sendResponse(session, new Response(Response.CREATED, Response.EMPTY));
                        sessionSent.set(true);
                    } else if (doneCount.get() - goodCount.get() > data.rf.getFrom() - data.rf.getAck()) {
                        sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                        sessionSent.set(true);
                    }
                }
            }).exceptionally(e -> {
                log.error("Error in schedulePut", e);
                return null;
            });
        }
    }

    void scheduleDeleteEntity(@NotNull final HttpSession session,
                              @NotNull final RequestData data) {
        final AtomicInteger goodCount = new AtomicInteger(0);
        final AtomicInteger doneCount = new AtomicInteger(0);
        final AtomicBoolean sessionSent = new AtomicBoolean(false);

        for (final String node : data.nodes) {
            CompletableFuture.runAsync(() -> {
                if (ResponseUtils.is2XX(proxy(node, data.request))) {
                    goodCount.incrementAndGet();
                } else if (topology.isMe(node)) {
                    final Response response = LocalClient.deleteMethod(dao, data.key);
                    if (ResponseUtils.is2XX(response)) {
                        goodCount.incrementAndGet();
                    }
                }
                doneCount.incrementAndGet();
            }, executor).thenAccept(v -> {
                if (!sessionSent.get()) {
                    if (goodCount.get() >= data.rf.getAck()) {
                        sendResponse(session, new Response(Response.ACCEPTED, Response.EMPTY));
                        sessionSent.set(true);
                    } else if (doneCount.get() - goodCount.get() > data.rf.getFrom() - data.rf.getAck()) {
                        sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                        sessionSent.set(true);
                    }
                }
            }).exceptionally(e -> {
                log.error("Error in scheduleDelete", e);
                return null;
            });
        }
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

    static class RequestData {
        final Request request;
        final ByteBuffer key;
        final ReplicationFactor rf;
        final Set<String> nodes;

        RequestData(@NotNull final Request request,
                    @NotNull final ByteBuffer key,
                    @NotNull final ReplicationFactor rf,
                    @NotNull final Set<String> nodes) {
            this.request = request;
            this.key = key;
            this.rf = rf;
            this.nodes = nodes;
        }
    }
}
