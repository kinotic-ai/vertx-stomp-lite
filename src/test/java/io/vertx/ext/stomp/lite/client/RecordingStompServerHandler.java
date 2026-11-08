package io.vertx.ext.stomp.lite.client;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.lite.AbstractStompServerHandler;
import io.vertx.ext.stomp.lite.frame.Frame;
import io.vertx.ext.stomp.lite.frame.Headers;
import io.vertx.ext.web.RoutingContext;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A server handler for the client tests: it answers the handshake and CONNECT as the test decides, records the
 * frames the client sends, receipts them, refuses a SEND to {@link #REFUSED_DESTINATION} with an ERROR frame, and
 * lets the test write MESSAGE frames to the client.
 */
public class RecordingStompServerHandler extends AbstractStompServerHandler {

    public static final String REFUSED_DESTINATION = "/queue/refused";

    private final Supplier<Future<MultiMap>> handshakeAnswer;
    private final Function<Map<String, String>, Future<Map<String, String>>> connectAnswer;
    private final LinkedBlockingQueue<Frame> sendFrames = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Frame> subscribeFrames = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Frame> unsubscribeFrames = new LinkedBlockingQueue<>();
    private final CompletableFuture<Void> disconnected = new CompletableFuture<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final AtomicInteger closedCalls = new AtomicInteger();
    private volatile Runnable onClosed = () -> {};
    private volatile Runnable onException = () -> {};

    public RecordingStompServerHandler(Supplier<Future<MultiMap>> handshakeAnswer,
                                       Function<Map<String, String>, Future<Map<String, String>>> connectAnswer) {
        this.handshakeAnswer = handshakeAnswer;
        this.connectAnswer = connectAnswer;
    }

    @Override
    public Future<MultiMap> handshake(RoutingContext routingContext) {
        return handshakeAnswer.get();
    }

    @Override
    public Future<Map<String, String>> connect(Map<String, String> connectHeaders) {
        return connectAnswer.apply(connectHeaders);
    }

    @Override
    public void send(Frame frame) {
        if (REFUSED_DESTINATION.equals(frame.getDestination())) {
            stompServerConnection.sendErrorAndDisconnect(new IllegalStateException("refused"), frame);
        } else {
            sendFrames.add(frame);
            stompServerConnection.sendReceiptIfNeeded(frame);
        }
    }

    @Override
    public void subscribe(Frame frame) {
        subscribeFrames.add(frame);
        stompServerConnection.sendReceiptIfNeeded(frame);
    }

    @Override
    public void unsubscribe(Frame frame) {
        unsubscribeFrames.add(frame);
    }

    @Override
    public void begin(Frame frame) {
    }

    @Override
    public void abort(Frame frame) {
    }

    @Override
    public void commit(Frame frame) {
    }

    @Override
    public void ack(Frame frame) {
    }

    @Override
    public void nack(Frame frame) {
    }

    @Override
    public void exception(Throwable t) {
        onException.run();
    }

    @Override
    public void disconnected() {
        disconnected.complete(null);
    }

    @Override
    public void closed() {
        closedCalls.incrementAndGet();
        onClosed.run();
        closed.complete(null);
    }

    /**
     * Writes a MESSAGE frame to the client for the given subscription.
     */
    public Future<Void> message(String subscriptionId, String destination, Buffer body) {
        Headers headers = Headers.create(Frame.SUBSCRIPTION, subscriptionId,
                                         Frame.DESTINATION, destination,
                                         Frame.MESSAGE_ID, "m-" + System.nanoTime(),
                                         Frame.CONTENT_LENGTH, Integer.toString(body.length()));
        return stompServerConnection.write(new Frame(Frame.Command.MESSAGE, headers, body));
    }

    public LinkedBlockingQueue<Frame> sendFrames() {
        return sendFrames;
    }

    public LinkedBlockingQueue<Frame> subscribeFrames() {
        return subscribeFrames;
    }

    public LinkedBlockingQueue<Frame> unsubscribeFrames() {
        return unsubscribeFrames;
    }

    public CompletableFuture<Void> disconnectedSignal() {
        return disconnected;
    }

    public CompletableFuture<Void> closedSignal() {
        return closed;
    }

    /**
     * How many times the connection has called {@link #closed()}, which it must do exactly once.
     */
    public int closedCalls() {
        return closedCalls.get();
    }

    /**
     * Runs the action inside {@link #exception(Throwable)}, on the connection's context.
     */
    public void onException(Runnable action) {
        this.onException = action;
    }

    /**
     * Runs the action inside {@link #closed()}, on the connection's context, before the closed signal completes.
     */
    public void onClosed(Runnable action) {
        this.onClosed = action;
    }

}
