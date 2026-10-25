package io.vertx.ext.stomp.lite.client;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.http.WebSocketConnectOptions;

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
    private final Context context;
    private final StompClientOptions options;
    private final WebSocketClient webSocketClient;

    DefaultStompClient(Vertx vertx, StompClientOptions options) {
        Objects.requireNonNull(vertx, "vertx must not be null");
        Objects.requireNonNull(options, "options must not be null");
        this.vertx = vertx;
        this.context = vertx.getOrCreateContext();
        this.options = options;
        // the STOMP frame limit bounds the WebSocket frames and messages carrying the STOMP frames too
        WebSocketClientOptions webSocketClientOptions = new WebSocketClientOptions(options.getWebSocketClientOptions())
                .setMaxFrameSize(options.getMaxFrameSize())
                .setMaxMessageSize(options.getMaxFrameSize());
        this.webSocketClient = vertx.createWebSocketClient(webSocketClientOptions);
    }

    @Override
    public Future<StompClientConnection> connect(WebSocketConnectOptions webSocketOptions, Map<String, String> connectHeaders) {
        Objects.requireNonNull(webSocketOptions, "webSocketOptions must not be null");
        Objects.requireNonNull(connectHeaders, "connectHeaders must not be null");
        Promise<StompClientConnection> promise = Promise.promise();
        WebSocketConnectOptions effectiveOptions = new WebSocketConnectOptions(webSocketOptions);
        List<String> subProtocols = effectiveOptions.getSubProtocols();
        if (subProtocols == null || !subProtocols.contains(SUB_PROTOCOL)) {
            effectiveOptions.addSubProtocol(SUB_PROTOCOL);
        }
        // connecting from the client's context binds the socket, and so every handler, to that context
        context.runOnContext(v -> webSocketClient.connect(effectiveOptions)
                                                 .onFailure(promise::fail)
                                                 .onSuccess(webSocket -> {
                                                     String host = effectiveOptions.getHost() != null
                                                             ? effectiveOptions.getHost()
                                                             : webSocket.remoteAddress().host();
                                                     new DefaultStompClientConnection(vertx, webSocket, options)
                                                             .connect(host, connectHeaders, promise);
                                                 }));
        return promise.future();
    }

    @Override
    public Future<Void> close() {
        return webSocketClient.close();
    }

}
