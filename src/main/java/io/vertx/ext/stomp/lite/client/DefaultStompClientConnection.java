package io.vertx.ext.stomp.lite.client;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.ext.stomp.lite.StompServerOptions;
import io.vertx.ext.stomp.lite.frame.Frame;
import io.vertx.ext.stomp.lite.frame.FrameParser;
import io.vertx.ext.stomp.lite.frame.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Default {@link StompClientConnection} over a Vert.x {@link WebSocket}. Every method runs on the connection's
 * context: a call from another thread is dispatched to it.
 *
 * Created by Navid Mitchell on 2026-10-25.
 */
class DefaultStompClientConnection implements StompClientConnection {

    private static final Logger log = LoggerFactory.getLogger(DefaultStompClientConnection.class);

    private static final String ACCEPT_VERSION = "1.2";
    private static final String DISCONNECT_RECEIPT_PREFIX = "disconnect-";

    private final Vertx vertx;
    private final Context context;
    private final WebSocket webSocket;
    private final StompClientOptions options;
    private final FrameParser parser;
    private final Map<String, Handler<Frame>> subscriptions = new HashMap<>();
    private final Map<String, Promise<Void>> pendingReceipts = new HashMap<>();
    private final Promise<Void> closedPromise = Promise.promise();

    // the handlers are set from the caller's thread and read on the context; the connected frame and the
    // closed flag are read from any thread
    private volatile Handler<Frame> errorHandler;
    private volatile Handler<Throwable> closeHandler;
    private volatile Frame connectedFrame;
    private volatile boolean closed = false;
    private Promise<StompClientConnection> connectPromise;
    private Throwable closeCause;
    private long connectTimer = -1;
    private long outgoingHeartbeatTimer = -1;
    private long incomingHeartbeatTimer = -1;
    private long lastReadNanos;
    private long receiptSequence = 0;

    DefaultStompClientConnection(Vertx vertx, WebSocket webSocket, StompClientOptions options) {
        this.vertx = vertx;
        this.context = vertx.getOrCreateContext();
        this.webSocket = webSocket;
        this.options = options;
        // the parser enforces the body limit; the header limits stay at the server defaults
        this.parser = new FrameParser(new StompServerOptions().setMaxBodyLength(options.getMaxFrameSize()));
        this.parser.handler(this::handleFrame)
                   .errorHandler(this::fail);
    }

    /**
     * Sends the CONNECT frame and completes the promise with this connection once the CONNECTED frame arrives.
     *
     * @param host           the virtual host named in the CONNECT frame when the caller's headers name none
     * @param connectHeaders the caller's CONNECT headers
     * @param promise        completed with this connection, or failed with what ended the connection first, the
     *                       connect timeout included
     */
    void connect(String host, Map<String, String> connectHeaders, Promise<StompClientConnection> promise) {
        connectPromise = promise;
        lastReadNanos = System.nanoTime();
        webSocket.exceptionHandler(this::fail);
        webSocket.closeHandler(v -> handleClosed());
        webSocket.handler(buffer -> {
            lastReadNanos = System.nanoTime();
            try {
                parser.handle(buffer);
            } catch (Exception e) {
                fail(e);
            }
        });
        // a server that opened the socket and never answers CONNECT would otherwise hold the caller and the socket
        connectTimer = vertx.setTimer(options.getConnectTimeout(), id -> {
            connectTimer = -1;
            fail(new VertxException("The server did not answer the CONNECT frame within " + options.getConnectTimeout() + " ms", true));
        });
        Headers headers = Headers.create(connectHeaders);
        headers.put(Frame.ACCEPT_VERSION, ACCEPT_VERSION);
        headers.put(Frame.HEARTBEAT, Frame.Heartbeat.create(options.getHeartbeat()).toString());
        headers.putIfAbsent(Frame.HOST, host);
        write(new Frame(Frame.Command.CONNECT, headers, null))
                .onFailure(this::fail);
    }

    @Override
    public Frame connectedFrame() {
        return connectedFrame;
    }

    @Override
    public Future<Void> send(String destination, Map<String, String> headers, Buffer body) {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(headers, "headers must not be null");
        Headers frameHeaders = Headers.create(headers);
        frameHeaders.put(Frame.DESTINATION, destination);
        if (body != null && body.length() > 0) {
            frameHeaders.putIfAbsent(Frame.CONTENT_LENGTH, Integer.toString(body.length()));
        }
        return onContext(() -> writeAwaitingReceipt(new Frame(Frame.Command.SEND, frameHeaders, body)));
    }

    @Override
    public Future<Void> subscribe(String id, String destination, Map<String, String> headers, Handler<Frame> messageHandler) {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(headers, "headers must not be null");
        Objects.requireNonNull(messageHandler, "messageHandler must not be null");
        Headers frameHeaders = Headers.create(headers);
        frameHeaders.put(Frame.ID, id);
        frameHeaders.put(Frame.DESTINATION, destination);
        return onContext(() -> {
            subscriptions.put(id, messageHandler);
            return writeAwaitingReceipt(new Frame(Frame.Command.SUBSCRIBE, frameHeaders, null));
        });
    }

    @Override
    public Future<Void> unsubscribe(String id) {
        Objects.requireNonNull(id, "id must not be null");
        return onContext(() -> {
            subscriptions.remove(id);
            return write(new Frame(Frame.Command.UNSUBSCRIBE, Headers.create(Frame.ID, id), null));
        });
    }

    @Override
    public Future<Void> disconnect() {
        return onContext(() -> {
            if (!closed) {
                String receipt = DISCONNECT_RECEIPT_PREFIX + (++receiptSequence);
                // the server closes the connection after the RECEIPT, and a server that closes without one has
                // disconnected the client all the same, so either outcome ends in the close
                writeAwaitingReceipt(new Frame(Frame.Command.DISCONNECT, Headers.create(Frame.RECEIPT, receipt), null))
                        .onComplete(ar -> close());
            }
            return closedPromise.future();
        });
    }

    @Override
    public boolean isConnected() {
        return connectedFrame != null && !closed;
    }

    @Override
    public void close() {
        if (!closed && !webSocket.isClosed()) {
            webSocket.close();
        }
    }

    @Override
    public StompClientConnection errorHandler(Handler<Frame> handler) {
        this.errorHandler = handler;
        return this;
    }

    @Override
    public StompClientConnection closeHandler(Handler<Throwable> handler) {
        this.closeHandler = handler;
        return this;
    }

    private void handleFrame(Frame frame) {
        switch (frame.getCommand()) {
            case CONNECTED:
                handleConnected(frame);
                break;
            case MESSAGE:
                handleMessage(frame);
                break;
            case RECEIPT:
                handleReceipt(frame);
                break;
            case ERROR:
                handleError(frame);
                break;
            case PING:
                // read activity, which the data handler already recorded
                break;
            default:
                fail(new VertxException("The server sent a " + frame.getCommand() + " frame", true));
        }
    }

    private void handleConnected(Frame frame) {
        if (connectPromise == null) {
            fail(new VertxException("The server sent a second CONNECTED frame", true));
        } else {
            cancelConnectTimer();
            connectedFrame = frame;
            configureHeartbeat(Frame.Heartbeat.create(options.getHeartbeat()),
                               Frame.Heartbeat.parse(frame.getHeader(Frame.HEARTBEAT)));
            Promise<StompClientConnection> promise = connectPromise;
            connectPromise = null;
            promise.tryComplete(this);
        }
    }

    private void handleMessage(Frame frame) {
        Handler<Frame> handler = subscriptions.get(frame.getHeader(Frame.SUBSCRIPTION));
        if (handler != null) {
            handler.handle(frame);
        } else {
            log.warn("Dropping a MESSAGE frame for the unknown subscription {}", frame.getHeader(Frame.SUBSCRIPTION));
        }
    }

    private void handleReceipt(Frame frame) {
        Promise<Void> promise = pendingReceipts.remove(frame.getHeader(Frame.RECEIPT_ID));
        if (promise != null) {
            promise.complete();
        } else {
            log.warn("Dropping a RECEIPT frame for the unknown receipt {}", frame.getHeader(Frame.RECEIPT_ID));
        }
    }

    // The server closes the connection after an ERROR frame, so nothing pending can complete any more: the
    // operation the frame names by receipt-id fails with the error, the rest with the close that follows. A
    // frame naming no receipt fails every pending operation, since any of them may be the one it answers. The
    // connection is closed here too, so a server that fails to close is not waited on.
    private void handleError(Frame frame) {
        StompErrorException error = new StompErrorException(frame);
        if (closeCause == null) {
            closeCause = error;
        }
        failConnect(error);
        String receiptId = frame.getHeader(Frame.RECEIPT_ID);
        if (receiptId == null) {
            failPendingReceipts(error);
        } else {
            Promise<Void> promise = pendingReceipts.remove(receiptId);
            if (promise != null) {
                promise.fail(error);
            }
        }
        if (errorHandler != null) {
            errorHandler.handle(frame);
        }
        close();
    }

    private void configureHeartbeat(Frame.Heartbeat client, Frame.Heartbeat server) {
        long outgoingPeriod = Frame.Heartbeat.computeClientHeartbeatPeriod(client, server);
        long incomingPeriod = Frame.Heartbeat.computeServerHeartbeatPeriod(client, server);
        if (outgoingPeriod > 0) {
            // sent every period whatever else was written, so the gap the server measures stays one period and
            // never nears its cutoff of two
            outgoingHeartbeatTimer = vertx.setPeriodic(outgoingPeriod, id -> write(Buffer.buffer(FrameParser.EOL)));
        }
        if (incomingPeriod > 0) {
            // the same margin the server grants a client: two periods, so a heartbeat delayed by scheduling
            // does not end the connection
            incomingHeartbeatTimer = vertx.setPeriodic(incomingPeriod, id -> {
                long silentMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastReadNanos);
                if (silentMillis > incomingPeriod * 2) {
                    fail(new VertxException("No data received from the server in the last " + silentMillis
                                                    + " ms, the negotiated heartbeat is " + incomingPeriod + " ms", true));
                }
            });
        }
    }

    private void cancelConnectTimer() {
        if (connectTimer != -1) {
            vertx.cancelTimer(connectTimer);
            connectTimer = -1;
        }
    }

    private void cancelHeartbeat() {
        if (outgoingHeartbeatTimer != -1) {
            vertx.cancelTimer(outgoingHeartbeatTimer);
            outgoingHeartbeatTimer = -1;
        }
        if (incomingHeartbeatTimer != -1) {
            vertx.cancelTimer(incomingHeartbeatTimer);
            incomingHeartbeatTimer = -1;
        }
    }

    /**
     * Writes the frame; the result waits for the server's RECEIPT when the frame carries a receipt header.
     */
    private Future<Void> writeAwaitingReceipt(Frame frame) {
        Future<Void> ret;
        String receipt = frame.getReceipt();
        if (receipt == null) {
            ret = write(frame);
        } else if (closed) {
            ret = Future.failedFuture(closedException());
        } else if (pendingReceipts.containsKey(receipt)) {
            ret = Future.failedFuture(new IllegalStateException("A frame with the receipt " + receipt + " is still waiting for its RECEIPT"));
        } else {
            Promise<Void> promise = Promise.promise();
            pendingReceipts.put(receipt, promise);
            write(frame).onFailure(throwable -> {
                if (pendingReceipts.remove(receipt) != null) {
                    promise.fail(throwable);
                }
            });
            ret = promise.future();
        }
        return ret;
    }

    private Future<Void> write(Frame frame) {
        return write(frame.toBuffer());
    }

    private Future<Void> write(Buffer buffer) {
        Future<Void> ret;
        if (closed) {
            ret = Future.failedFuture(closedException());
        } else {
            ret = webSocket.writeBinaryMessage(buffer);
        }
        return ret;
    }

    // Ends the connection over a failure: the cause reaches whoever is waiting, through the close that follows
    private void fail(Throwable cause) {
        if (closeCause == null) {
            closeCause = cause;
        }
        close();
    }

    private void handleClosed() {
        if (!closed) {
            closed = true;
            cancelConnectTimer();
            cancelHeartbeat();
            Throwable cause = closeCause;
            failConnect(cause != null ? cause : new VertxException("The connection closed before the CONNECTED frame arrived", true));
            failPendingReceipts(cause != null
                                        ? new VertxException("The connection closed before the receipt arrived", cause, true)
                                        : closedException());
            subscriptions.clear();
            if (closeHandler != null) {
                try {
                    closeHandler.handle(cause);
                } catch (Exception e) {
                    log.error("The close handler failed", e);
                }
            }
            closedPromise.tryComplete();
        }
    }

    private void failConnect(Throwable cause) {
        if (connectPromise != null) {
            Promise<StompClientConnection> promise = connectPromise;
            connectPromise = null;
            promise.tryFail(cause);
        }
    }

    private void failPendingReceipts(Throwable cause) {
        List<Promise<Void>> pending = new ArrayList<>(pendingReceipts.values());
        pendingReceipts.clear();
        pending.forEach(promise -> promise.fail(cause));
    }

    private VertxException closedException() {
        return new VertxException("The connection is closed", true);
    }

    // The connection's state is only ever touched on its context
    private <T> Future<T> onContext(Supplier<Future<T>> operation) {
        Future<T> ret;
        if (Vertx.currentContext() == context) {
            ret = guarded(operation);
        } else {
            Promise<T> promise = Promise.promise();
            context.runOnContext(v -> guarded(operation).onComplete(promise));
            ret = promise.future();
        }
        return ret;
    }

    // a throw from the operation is the result's failure, from whichever thread the caller came
    private <T> Future<T> guarded(Supplier<Future<T>> operation) {
        Future<T> ret;
        try {
            ret = operation.get();
        } catch (Exception e) {
            ret = Future.failedFuture(e);
        }
        return ret;
    }

}
