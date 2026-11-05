package io.vertx.ext.stomp.lite.client;

import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.stomp.lite.StompServerOptions;
import io.vertx.ext.stomp.lite.frame.Frame;

import java.util.Objects;

/**
 * STOMP client options. The WebSocket transport, TLS included, is configured through the Vert.x
 * {@link WebSocketClientOptions} these options carry.
 *
 * Created by Navid Mitchell on 2026-10-25.
 */
public class StompClientOptions {

    public static final int DEFAULT_MAX_BODY_LENGTH = StompServerOptions.DEFAULT_MAX_BODY_LENGTH;
    public static final long DEFAULT_CONNECT_TIMEOUT = 10_000;

    private WebSocketClientOptions webSocketClientOptions = new WebSocketClientOptions();
    private int maxBodyLength = DEFAULT_MAX_BODY_LENGTH;
    private long connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private JsonObject heartbeat = new JsonObject().put("x", 30000).put("y", 30000);

    public StompClientOptions() {
    }

    /**
     * Copies the options.
     *
     * @param other the options to copy
     */
    public StompClientOptions(StompClientOptions other) {
        this.webSocketClientOptions = new WebSocketClientOptions(other.webSocketClientOptions);
        this.maxBodyLength = other.maxBodyLength;
        this.connectTimeout = other.connectTimeout;
        this.heartbeat = other.heartbeat.copy();
    }

    /**
     * Gets the options of the Vert.x WebSocket client that carries the STOMP frames, which hold the TLS
     * configuration.
     *
     * @return the WebSocket client options
     */
    public WebSocketClientOptions getWebSocketClientOptions() {
        return webSocketClientOptions;
    }

    /**
     * Sets the options of the Vert.x WebSocket client that carries the STOMP frames, the maximum WebSocket frame
     * size included: a server that writes a STOMP frame in one WebSocket frame needs that size to hold the frame
     * whole, and a Vert.x server fragments a frame at its own maximum.
     *
     * @param webSocketClientOptions the WebSocket client options, must not be {@code null}
     * @return the current {@link StompClientOptions}
     */
    public StompClientOptions setWebSocketClientOptions(WebSocketClientOptions webSocketClientOptions) {
        this.webSocketClientOptions = Objects.requireNonNull(webSocketClientOptions, "webSocketClientOptions must not be null");
        return this;
    }

    /**
     * Gets the size a STOMP frame body must stay under, in bytes. A body reaching it closes the connection with an
     * error. The WebSocket frames that carry a STOMP frame are bounded separately, by the WebSocket client options;
     * the frames the client writes are at most 64 KB, Vert.x's default limit. 10 MB by default.
     *
     * @return the maximum body length in bytes
     */
    public int getMaxBodyLength() {
        return maxBodyLength;
    }

    /**
     * Sets the size a STOMP frame body must stay under, in bytes.
     *
     * @param maxBodyLength the maximum body length in bytes, at least 1
     * @return the current {@link StompClientOptions}
     */
    public StompClientOptions setMaxBodyLength(int maxBodyLength) {
        if (maxBodyLength < 1) {
            throw new IllegalArgumentException("maxBodyLength must be at least 1 byte but was " + maxBodyLength);
        }
        this.maxBodyLength = maxBodyLength;
        return this;
    }

    /**
     * Gets the time the server has to answer the CONNECT frame once the WebSocket is open, and the DISCONNECT
     * frame of {@link StompClientConnection#disconnect()}, in milliseconds. A connection the server has not
     * accepted by then fails and its WebSocket is closed; a disconnect it has not answered by then closes the
     * WebSocket. 10 s by default.
     *
     * @return the connect timeout in milliseconds
     */
    public long getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Sets the time the server has to answer the CONNECT frame once the WebSocket is open, and the DISCONNECT
     * frame on disconnect.
     *
     * @param connectTimeout the connect timeout in milliseconds, at least 1
     * @return the current {@link StompClientOptions}
     */
    public StompClientOptions setConnectTimeout(long connectTimeout) {
        if (connectTimeout < 1) {
            throw new IllegalArgumentException("connectTimeout must be at least 1 ms but was " + connectTimeout);
        }
        this.connectTimeout = connectTimeout;
        return this;
    }

    /**
     * Gets the heartbeat the client offers in its CONNECT frame: {@code x} is the interval in milliseconds at
     * which the client can send heartbeats, {@code y} the interval at which it wants to receive them. Either can
     * be {@code 0} to turn that direction off. Defaults to 30 s both ways.
     *
     * @return the heartbeat configuration
     * @see Frame.Heartbeat
     */
    public JsonObject getHeartbeat() {
        return heartbeat;
    }

    /**
     * Sets the heartbeat the client offers in its CONNECT frame.
     *
     * @param heartbeat the heartbeat configuration, {@code {"x": millis, "y": millis}}
     * @return the current {@link StompClientOptions}
     * @see Frame.Heartbeat
     */
    public StompClientOptions setHeartbeat(JsonObject heartbeat) {
        this.heartbeat = Objects.requireNonNull(heartbeat, "heartbeat must not be null");
        return this;
    }

}
