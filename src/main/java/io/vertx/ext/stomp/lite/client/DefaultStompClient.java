package io.vertx.ext.stomp.lite.client;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.internal.ContextInternal;
import io.vertx.ext.stomp.lite.frame.Frame;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Default {@link StompClient}: one Vert.x {@link WebSocketClient} shared by every connection, bound to the context
 * the client was created on.
 *
 * Created by Navid Mitchell on 2026-10-25.
 */
class DefaultStompClient implements StompClient {

    private final Vertx vertx;
    private final ContextInternal context;
    private final StompClientOptions options;
    private final WebSocketClient webSocketClient;

    DefaultStompClient(Vertx vertx, StompClientOptions options) {
        Objects.requireNonNull(vertx, "vertx must not be null");
        Objects.requireNonNull(options, "options must not be null");
        this.vertx = vertx;
        this.context = (ContextInternal) vertx.getOrCreateContext();
        this.options = options;
        // the STOMP frame limit bounds the WebSocket frames carrying the STOMP frames too
        WebSocketClientOptions webSocketClientOptions = new WebSocketClientOptions(options.getWebSocketClientOptions())
                .setMaxFrameSize(options.getMaxFrameSize());
        this.webSocketClient = vertx.createWebSocketClient(webSocketClientOptions);
    }

    @Override
    public Future<StompClientConnection> connect(WebSocketConnectOptions webSocketOptions, Map<String, String> connectHeaders) {
        Objects.requireNonNull(webSocketOptions, "webSocketOptions must not be null");
        Objects.requireNonNull(connectHeaders, "connectHeaders must not be null");
        // a promise of the context reports a throw from the caller's handler to the context; a context-less one
        // would drop it
        Promise<StompClientConnection> promise = context.promise();
        WebSocketConnectOptions effectiveOptions = new WebSocketConnectOptions(webSocketOptions);
        // the copy shares the caller's sub-protocol list, so the sub-protocol is added to a list of the client's own
        List<String> subProtocols = new ArrayList<>(effectiveOptions.getSubProtocols() != null
                                                            ? effectiveOptions.getSubProtocols()
                                                            : List.of());
        if (!subProtocols.contains(Frame.WEBSOCKET_SUB_PROTOCOL)) {
            subProtocols.add(Frame.WEBSOCKET_SUB_PROTOCOL);
        }
        effectiveOptions.setSubProtocols(subProtocols);
        // connecting from the client's context binds the socket, and so every handler, to that context
        context.runOnContext(v -> webSocketClient.connect(effectiveOptions)
                                                 .onFailure(promise::fail)
                                                 .onSuccess(webSocket -> {
                                                     try {
                                                         String host = effectiveOptions.getHost() != null
                                                                 ? effectiveOptions.getHost()
                                                                 : webSocket.remoteAddress().host();
                                                         new DefaultStompClientConnection(vertx, webSocket, options)
                                                                 .connect(host, connectHeaders, promise);
                                                     } catch (Exception e) {
                                                         // the socket is open and nothing else holds it
                                                         webSocket.close();
                                                         promise.tryFail(e);
                                                     }
                                                 }));
        return promise.future();
    }

    @Override
    public Future<Void> close() {
        return webSocketClient.close();
    }

}
