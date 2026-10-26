package io.vertx.ext.stomp.lite.client;

import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.stomp.lite.StompServerOptions;
import io.vertx.ext.stomp.lite.frame.Frame;

/**
 * STOMP client options. The WebSocket transport, TLS included, is configured through the Vert.x
 * {@link WebSocketClientOptions} these options carry.
 *
 * Created by Navid Mitchell on 2026-10-25.
 */
public class StompClientOptions {

    public static final int DEFAULT_MAX_FRAME_SIZE = StompServerOptions.DEFAULT_MAX_BODY_LENGTH;

    private WebSocketClientOptions webSocketClientOptions = new WebSocketClientOptions();
    private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    private JsonObject heartbeat = new JsonObject().put("x", 30000).put("y", 30000);

    /**
     * Gets the options of the Vert.x WebSocket client that carries the STOMP frames, which hold the TLS
     * configuration and the connect timeout.
     *
     * @return the WebSocket client options
     */
    public WebSocketClientOptions getWebSocketClientOptions() {
        return webSocketClientOptions;
    }

    /**
     * Sets the options of the Vert.x WebSocket client that carries the STOMP frames. The maximum WebSocket frame
     * and message sizes are set from {@link #getMaxFrameSize()} when the client is created.
     *
     * @param webSocketClientOptions the WebSocket client options, must not be {@code null}
     * @return the current {@link StompClientOptions}
     */
    public StompClientOptions setWebSocketClientOptions(WebSocketClientOptions webSocketClientOptions) {
        this.webSocketClientOptions = webSocketClientOptions;
        return this;
    }

    /**
     * Gets the size a STOMP frame body must stay under, in bytes. A body reaching it, or a WebSocket frame
     * exceeding it, closes the connection with an error. 10 MB by default.
     *
     * @return the maximum frame size in bytes
     */
    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    /**
     * Sets the size a STOMP frame body must stay under, in bytes.
     *
     * @param maxFrameSize the maximum frame size in bytes
     * @return the current {@link StompClientOptions}
     */
    public StompClientOptions setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
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
        this.heartbeat = heartbeat;
        return this;
    }

}
