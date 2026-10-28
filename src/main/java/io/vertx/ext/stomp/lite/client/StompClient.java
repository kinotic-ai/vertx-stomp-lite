package io.vertx.ext.stomp.lite.client;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.ext.stomp.lite.frame.Frame;

import java.util.Map;

/**
 * A STOMP 1.2 client over WebSocket. The client runs entirely on the Vert.x context it was created on, so its
 * handlers are invoked there and never block it; every result is a Vert.x {@link Future}. Reconnecting after a
 * connection is lost is the caller's decision: the client reports the loss through
 * {@link StompClientConnection#closeHandler} and the caller connects again when it sees fit.
 *
 * Created by Navid Mitchell on 2026-10-25.
 */
public interface StompClient {

    /**
     * Creates a client bound to the current Vert.x context, or to a new one when there is none.
     *
     * @param vertx   the Vert.x instance
     * @param options the client options
     * @return the client
     */
    static StompClient create(Vertx vertx, StompClientOptions options) {
        return new DefaultStompClient(vertx, options);
    }

    /**
     * Opens a WebSocket to the server under the {@link Frame#WEBSOCKET_SUB_PROTOCOL} sub-protocol and sends a
     * CONNECT frame. The frame carries the given headers plus the protocol headers the client owns:
     * {@code accept-version}, {@code heart-beat} and, when the caller supplied none, {@code host}.
     *
     * @param webSocketOptions where to connect, with any headers the WebSocket upgrade request must carry
     * @param connectHeaders  the headers of the CONNECT frame, such as {@code login} and {@code passcode}
     * @return a future completed with the connection once the CONNECTED frame arrives, or failed with the
     *         {@link StompErrorException} of the ERROR frame the server answered instead, with the failure of the
     *         WebSocket connection, or once the server has not answered the CONNECT frame within
     *         {@link StompClientOptions#getConnectTimeout()}
     */
    Future<StompClientConnection> connect(WebSocketConnectOptions webSocketOptions, Map<String, String> connectHeaders);

    /**
     * Closes the client and the WebSocket client it holds. Connections still open are closed with it.
     *
     * @return a future completed when the client is closed
     */
    Future<Void> close();

}
