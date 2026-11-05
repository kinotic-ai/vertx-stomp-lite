package io.vertx.ext.stomp.lite.client;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.internal.ContextInternal;
import io.vertx.ext.stomp.lite.StompServerOptions;
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

    private final ContextInternal context;
    private final StompClientOptions options;
    private final StompServerOptions parserOptions;
    private final WebSocketClient webSocketClient;

    DefaultStompClient(Vertx vertx, StompClientOptions options) {
        Objects.requireNonNull(vertx, "vertx must not be null");
        Objects.requireNonNull(options, "options must not be null");
        this.context = (ContextInternal) vertx.getOrCreateContext();
        // copied once, so every connection of the client is made with the same options
        this.options = new StompClientOptions(options);
        // the parser enforces the body limit, with the header limits at the server defaults; the WebSocket frame
        // size is the transport's own, on the WebSocket client options
        this.parserOptions = new StompServerOptions().setMaxBodyLength(this.options.getMaxBodyLength());
        this.webSocketClient = vertx.createWebSocketClient(this.options.getWebSocketClientOptions());
    }

    @Override
    public Future<StompClientConnection> connect(WebSocketConnectOptions webSocketOptions, Map<String, String> connectHeaders) {
        Objects.requireNonNull(webSocketOptions, "webSocketOptions must not be null");
        Objects.requireNonNull(connectHeaders, "connectHeaders must not be null");
        // a promise of the context reports a throw from the caller's handler to the context; a context-less one
        // would drop it
        Promise<StompClientConnection> promise = context.promise();
        WebSocketConnectOptions effectiveOptions = new WebSocketConnectOptions(webSocketOptions);
        // a server that echoes no sub-protocol fails a handshake that requested one, so a caller naming its own,
        // none included, is honoured as is
        if (effectiveOptions.getSubProtocols() == null) {
            effectiveOptions.setSubProtocols(new ArrayList<>(List.of(Frame.WEBSOCKET_SUB_PROTOCOL)));
        }
        // connecting from the client's context binds the socket, and so every handler, to that context
        context.runOnContext(v -> webSocketClient.connect(effectiveOptions)
                                                 .onFailure(promise::fail)
                                                 .onSuccess(webSocket -> {
                                                     try {
                                                         String host = effectiveOptions.getHost() != null
                                                                 ? effectiveOptions.getHost()
                                                                 : webSocket.remoteAddress().host();
                                                         new DefaultStompClientConnection(context, webSocket, options, parserOptions)
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
