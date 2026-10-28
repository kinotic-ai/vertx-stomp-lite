package io.vertx.ext.stomp.lite.handler;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.stomp.lite.StompServerConnection;
import io.vertx.ext.stomp.lite.StompServerHandler;
import io.vertx.ext.stomp.lite.StompServerOptions;
import io.vertx.ext.stomp.lite.frame.Frame;
import io.vertx.ext.stomp.lite.frame.FrameParser;
import io.vertx.ext.stomp.lite.frame.Frames;
import io.vertx.ext.stomp.lite.frame.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSession;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Navid Mitchell on 2019-01-10.
 */
class DefaultStompServerConnection implements Handler<Frame>, StompServerConnection {

    private static final Logger log = LoggerFactory.getLogger(DefaultStompServerConnection.class);

    private final ServerWebSocket serverWebSocket;
    private final Vertx vertx;
    private final StompServerOptions options;
    private final StompServerHandler stompServerHandler;

    private boolean connected = false;
    private boolean closed = false;
    private volatile long lastClientActivity;
    private long serverHeartbeat = -1;
    private long clientHeartbeat = -1;


    DefaultStompServerConnection(ServerWebSocket serverWebSocket,
                                 Vertx vertx,
                                 StompServerOptions options,
                                 StompServerHandler stompServerHandler) {
        this.serverWebSocket = serverWebSocket;
        this.vertx = vertx;
        this.options = options;
        this.stompServerHandler = stompServerHandler;
        this.stompServerHandler.connectionCreated(this);

        if(log.isDebugEnabled()){
            log.debug("New Stomp Connection. Host: {}", serverWebSocket.remoteAddress().host());
        }
    }

    @Override
    public String binaryHandlerID() {
        return serverWebSocket.binaryHandlerID();
    }

    @Override
    public String textHandlerID() {
        return serverWebSocket.textHandlerID();
    }

    @Override
    public SSLSession sslSession() {
        return serverWebSocket.sslSession();
    }

    @Override
    public SocketAddress remoteAddress() {
        return serverWebSocket.remoteAddress();
    }

    @Override
    public SocketAddress localAddress() {
        return serverWebSocket.localAddress();
    }

    @Override
    public boolean isSsl() {
        return serverWebSocket.isSsl();
    }

    @Override
    public Future<Void> write(Frame frame) {
        return write(frame.toBuffer(options.isTrailingLine()));
    }

    @Override
    public Future<Void> write(Buffer buffer) {
        return serverWebSocket.writeBinaryMessage(buffer);
    }

    @Override
    public Future<Void> sendReceiptIfNeeded(Frame frame) {
        String receipt = frame.getReceipt();
        if (receipt != null) {
            return write(Frames.createReceiptFrame(receipt, Headers.create()));
        }else{
            return Future.succeededFuture();
        }
    }

    @Override
    public Future<Void> sendError(Throwable throwable) {
        return write(Frames.createErrorFrame(throwable, options.isDebugEnabled()));
    }

    @Override
    public Future<Void> sendErrorAndDisconnect(Throwable throwable) {
        return sendErrorAndDisconnect(throwable, null);
    }

    @Override
    public Future<Void> sendErrorAndDisconnect(Throwable throwable, Frame causingFrame) {
        if(log.isDebugEnabled()){
            log.debug("Sending Error and disconnecting client. Host: {}", serverWebSocket.remoteAddress().host(), throwable);
        }
        Frame error = Frames.createErrorFrame(throwable, options.isDebugEnabled());
        String receipt = causingFrame != null ? causingFrame.getReceipt() : null;
        if (receipt != null) {
            error.addHeader(Frame.RECEIPT_ID, receipt);
        }
        Promise<Void> ret = Promise.promise();
        write(error)
                .onComplete(event -> {
                    // now that data was sent close connection and finish promise
                    close();
                    if (event.succeeded()) {
                        ret.complete();
                    } else {
                        ret.fail(event.cause());
                    }
                });
        return ret.future();
    }

    /**
     * Used to signal that an exception occurred before a client frame could be processed: the connection is closed
     * without an ERROR frame
     * @param t the exception that was caused
     */
    public void clientCausedException(Throwable t){
        notifyException(t);
        close();
    }

    /**
     * Used to signal that the processing of a client frame resulted in an exception: the client receives an ERROR
     * frame naming that frame's receipt, then the connection is closed
     * @param t the exception that was caused
     * @param causingFrame the frame whose processing failed
     */
    public void clientCausedException(Throwable t, Frame causingFrame){
        notifyException(t);
        logIfFailed(sendErrorAndDisconnect(t, causingFrame),
                    "Problem sending ERROR frame to client");
    }

    private void notifyException(Throwable t) {
        try {
            stompServerHandler.exception(t);
        } catch (Exception e) {
            log.error("StompServerHandler.exception handler threw an exception.. You should fix your handler not to throw exceptions.", e);
        }
    }

    @Override
    public void pause() {
        if(!closed) {
            serverWebSocket.pause();
        }
    }

    @Override
    public void resume() {
        if(!closed) {
            serverWebSocket.resume();
        }
    }

    @Override
    public void fetch(long amount) {
        if(!closed) {
            serverWebSocket.fetch(amount);
        }
    }

    @Override
    public void close() {
        if(!closed) {
            if(log.isDebugEnabled()) {
                log.debug("Closing Stomp Connection. Host: {}", serverWebSocket.remoteAddress().host());
            }

            connected = false;

            try {
                cancelHeartbeat();
            } catch (Exception e) {
                log.error("StompServerHandler unhandled error on cancelHeartbeat", e);
            }

            try {

                //*** This must be called under all circumstances so the Handler can clean up any client subscriptions ***
                stompServerHandler.closed();

            } catch (Exception e) {
                log.error("StompServerHandler.disconnected() handler threw an exception.. You should fix your handler not to throw exceptions.", e);
            }

            try {
                if(!serverWebSocket.isClosed()) {
                    serverWebSocket.close();
                }
            } catch (Exception e) {
                // Ignore it, the web socket has already been closed.
                log.warn("Error closing serverWebSocket.", e);
            }
            closed = true;
        }
    }


    /****                                                                                                       ****
     ****                                            Handler Logic                                              ****
     ****                                                                                                       ****/

    @Override
    public void handle(Frame frame) {
        if(!closed) {
            try {
                switch (frame.getCommand()) {
                    case CONNECT:
                        if(connected){
                            clientCausedException(new IllegalStateException("CONNECT has already been called."), frame);
                        } else {
                            onConnect(frame);
                        }
                        break;
                    case SEND:
                        ensureConnected();
                        onClientActivity();
                        try {
                            stompServerHandler.send(frame);
                        } catch (Exception e) {
                            log.error("StompServerHandler.send handler threw an exception.. You should fix your handler not to throw exceptions.", e);
                        }
                        break;
                    case SUBSCRIBE:
                        ensureConnected();
                        onClientActivity();
                        try {
                            stompServerHandler.subscribe(frame);
                        } catch (Exception e) {
                            log.error("StompServerHandler.subscribe handler threw an exception.. You should fix your handler not to throw exceptions.", e);
                        }
                        break;
                    case UNSUBSCRIBE:
                        ensureConnected();
                        onClientActivity();
                        try {
                            stompServerHandler.unsubscribe(frame);
                        } catch (Exception e) {
                            log.error("StompServerHandler.unsubscribe handler threw an exception.. You should fix your handler not to throw exceptions.", e);
                        }
                        break;
                    case BEGIN:
                        ensureConnected();
                        onClientActivity();
                        try {
                            stompServerHandler.begin(frame);
                        } catch (Exception e) {
                            log.error("StompServerHandler.begin handler threw an exception.. You should fix your handler not to throw exceptions.", e);
                        }
                        break;
                    case ABORT:
                        ensureConnected();
                        onClientActivity();
                        try {
                            stompServerHandler.abort(frame);
                        } catch (Exception e) {
                            log.error("StompServerHandler.abort handler threw an exception.. You should fix your handler not to throw exceptions.", e);
                        }
                        break;
                    case COMMIT:
                        ensureConnected();
                        onClientActivity();
                        try {
                            stompServerHandler.commit(frame);
                        } catch (Exception e) {
                            log.error("StompServerHandler.commit handler threw an exception.. You should fix your handler not to throw exceptions.", e);
                        }
                        break;
                    case ACK:
                        ensureConnected();
                        onClientActivity();
                        try {
                            stompServerHandler.ack(frame);
                        } catch (Exception e) {
                            log.error("StompServerHandler.ack handler threw an exception.. You should fix your handler not to throw exceptions.", e);
                        }
                        break;
                    case NACK:
                        ensureConnected();
                        onClientActivity();
                        try {
                            stompServerHandler.nack(frame);
                        } catch (Exception e) {
                            log.error("StompServerHandler.nack handler threw an exception.. You should fix your handler not to throw exceptions.", e);
                        }
                        break;
                    case DISCONNECT:
                        ensureConnected();
                        onClientActivity();
                        sendReceiptIfNeeded(frame);
                        try {
                            stompServerHandler.disconnected();
                        } catch (Exception e) {
                            log.error("StompServerHandler.disconnected handler threw an exception.. You should fix your handler not to throw exceptions.", e);
                        }
                        close();
                        break;
                    case PING:
                        ensureConnected();
                        onClientActivity(); // we just increment activity stomp pings do not expect a response
                        break;
                    default:
                        throw new IllegalStateException("Unknown command");
                }
            } catch (Exception e) {
                // Sync throws from the user StompServerHandler (e.g. connect()) must produce
                // a STOMP ERROR frame so the client sees a diagnostic instead of a bare WS close.
                clientCausedException(e, frame);
            }
        } else {
            log.error("THIS SHOULD NEVER HAPPEN!! Frame Handler called after close.");
        }
    }

    private void ensureConnected() {
        if (!connected) {
            throw new IllegalStateException("Client must provide a connect frame before any other frames");
        }
    }

    public boolean isConnected() {
        return connected;
    }

    private void onConnect(Frame frame) {
        // Server negotiation
        List<String> accepted = new ArrayList<>();
        String accept = frame.getHeader(Frame.ACCEPT_VERSION);
        if (accept == null) {
            accepted.add("1.2");
        } else {
            accepted.addAll(Arrays.asList(accept.split(FrameParser.COMMA)));
        }

        String version = negotiate(accepted);
        if (version == null) {
            // Spec says: if the server and the client do not share any common protocol versions, then the server MUST respond with an error.
            throw new IllegalStateException("Client protocol requirement does not mach versions supported by the server.");
        }

        // Now process the client CONNECT frame
        stompServerHandler
                .connect(frame.getHeaders())
                .onComplete(connectResult -> {

                    if (connectResult.succeeded()) {

                        Headers headers = Headers.create(connectResult.result());
                        headers.add(Frame.VERSION,
                                    version); // Spec says: The server will respond back with the highest version of the protocol -> version
                        headers.add(Frame.HEARTBEAT, Frame.Heartbeat.create(options.getHeartbeat()).toString());

                        write(new Frame(Frame.Command.CONNECTED, headers, null))
                                .onComplete(ar -> {
                                    if (ar.succeeded()) {
                                        // now that we are connected Compute heartbeat, and register serverHeartbeat and clientHeartbeat
                                        Frame.Heartbeat clientHeartbeat = Frame.Heartbeat.parse(frame.getHeader(Frame.HEARTBEAT));
                                        Frame.Heartbeat serverHeartbeat = Frame.Heartbeat.create(options.getHeartbeat());
                                        long clientHeartbeatPeriod = Frame.Heartbeat.computeClientHeartbeatPeriod(clientHeartbeat,
                                                                                                                  serverHeartbeat);
                                        long serverHeartbeatPeriod = Frame.Heartbeat.computeServerHeartbeatPeriod(clientHeartbeat,
                                                                                                                  serverHeartbeat);

                                        onClientActivity();

                                        configureHeartbeat(clientHeartbeatPeriod, serverHeartbeatPeriod);

                                        if (log.isDebugEnabled()) {
                                            log.debug("Stomp client authenticated. Host: {}",
                                                      serverWebSocket.remoteAddress().host());
                                        }

                                        connected = true;
                                    } else {
                                        if (log.isDebugEnabled()) {
                                            log.debug("Could not send CONNECTED frame. Host: {}",
                                                      serverWebSocket.remoteAddress().host(),
                                                      ar.cause());
                                        }
                                        close(); // cleanup
                                    }
                                });

                    } else {
                        logIfFailed(sendErrorAndDisconnect(connectResult.cause()),
                                    "Problem Sending Authentication Error to client");
                    }
                });
    }

    private String negotiate(List<String> accepted) {
        List<String> supported = Collections.singletonList("1.2");
        for (String v : supported) {
            if (accepted.contains(v)) {
                return v;
            }
        }
        return null;
    }

    private void cancelHeartbeat() {
        if (serverHeartbeat >= 0) {
            vertx.cancelTimer(serverHeartbeat);
            serverHeartbeat = -1;
        }

        if (clientHeartbeat >= 0) {
            vertx.cancelTimer(clientHeartbeat);
            clientHeartbeat = -1;
        }
    }

    private void onClientActivity() {
        lastClientActivity = System.nanoTime();
    }

    private void ping() {
        serverWebSocket.writeBinaryMessage(Frames.ping().toBuffer());
    }

    private void configureHeartbeat(long clientHeartbeatPeriod, long serverHeartbeatPeriod) {
        if (serverHeartbeatPeriod > 0) {
            // sent every period whatever else was written, so the gap the client measures stays one period and
            // never nears its cutoff of two
            serverHeartbeat = vertx.setPeriodic(serverHeartbeatPeriod, event -> ping());
        }
        if (clientHeartbeatPeriod > 0) {
            clientHeartbeat = vertx.setPeriodic(clientHeartbeatPeriod, l -> {
                long delta = System.nanoTime() - lastClientActivity;
                final long deltaInMs = TimeUnit.MILLISECONDS.convert(delta, TimeUnit.NANOSECONDS);
                if (deltaInMs > clientHeartbeatPeriod * 2) {
                    if (log.isDebugEnabled()) {
                        log.debug("Disconnecting client {} - no client activity in the last {} ms",
                                  serverWebSocket.remoteAddress().host(),
                                  deltaInMs);
                    }
                    close();
                }
            });
        }
    }

    private void logIfFailed(Future<Void> future, String message){
        if(log.isDebugEnabled()){
            future.onComplete(event -> {
                if (event.failed()) {
                    log.debug(message, event.cause());
                }
            });
        }
    }

}
