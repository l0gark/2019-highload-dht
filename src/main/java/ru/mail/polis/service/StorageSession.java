package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.persistence.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class StorageSession extends HttpSession {

    private static final byte[] CRLF = "\r\n".getBytes(Charsets.UTF_8);
    private static final byte[] LF = "\n".getBytes(Charsets.UTF_8);
    private static final byte[] EMPTY_CHUNK = "0\r\n\r\n".getBytes(Charsets.UTF_8);
    private static final Logger log = LoggerFactory.getLogger(StorageSession.class);

    private Iterator<Record> records;

    StorageSession(@NotNull Socket socket, @NotNull HttpServer server) {
        super(socket, server);
    }

    void stream(@NotNull final Iterator<Record> records) throws IOException {
        this.records = records;

        final Response response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response, false);
        next();
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        next();
    }

    private void next() throws IOException {
        if (records == null) {
            throw new IllegalStateException("Data can't be null");
        }
        while (records.hasNext() && queueHead == null) {
            final Record record = records.next();
            final byte[] key = Bytes.toArray(record.getKey());
            final byte[] value = Bytes.toArray(record.getValue());
            // <key>'\n'<value>
            final int payloadLength = key.length + LF.length + value.length;
            final String size = Integer.toHexString(payloadLength);
            // <size>\r\n<payload>\r\n
            final int chunkLength = size.length() + CRLF.length + payloadLength + CRLF.length;
            //Build chunk
            buildChunk(size, key, value, chunkLength);
        }
        if (!records.hasNext()) {
            write(EMPTY_CHUNK, 0, EMPTY_CHUNK.length);

            server.incRequestsProcessed();

            if ((handling = pipeline.pollFirst()) != null) {
                if (handling == FIN) {
                    scheduleClose();
                } else {
                    try {
                        server.handleRequest(handling, this);
                    } catch (IOException e) {
                        log.error("Can't process next request" + handling, e);
                    }
                }
            }
        }
    }

    private void buildChunk(@NotNull final String size,
                            final byte[] key,
                            final byte[] value,
                            final int chunkLength) throws IOException {
        final byte[] chunk = new byte[chunkLength];
        final ByteBuffer buffer = ByteBuffer.wrap(chunk);
        buffer.put(size.getBytes(Charsets.UTF_8));
        buffer.put(CRLF);
        buffer.put(key);
        buffer.put(LF);
        buffer.put(value);
        buffer.put(CRLF);
        write(chunk, 0, chunk.length);
    }
}
