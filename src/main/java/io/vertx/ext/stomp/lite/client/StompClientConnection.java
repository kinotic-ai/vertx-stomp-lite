package io.vertx.ext.stomp.lite.client;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.lite.frame.Frame;

import java.util.Map;

/**
 * A connection the server accepted with a CONNECTED frame. Frames are written in the order the operations are
 * called, and a frame that carries a {@code receipt} header completes its operation when the server's RECEIPT
 * arrives rather than when the frame is written.
 *
 * Created by Navid Mitchell on 2026-10-25.
 */
public interface StompClientConnection {

    /**
     * @return the CONNECTED frame the server accepted the connection with
     */
    Frame connectedFrame();

    /**
     * Sends a SEND frame. A non-empty body is sent with its {@code content-length}, so binary data survives a
     * {@code NULL} byte.
     *
     * @param destination the destination header
     * @param headers     the other headers of the frame, may be empty
     * @param body        the body, or {@code null} for none
     * @return a future completed when the frame is written, or when the RECEIPT for the frame's {@code receipt}
     *         header arrives; failed with the ERROR frame's {@link StompErrorException}, when the connection
     *         closes first, or at once when another frame is still waiting for a RECEIPT of the same id
     */
    Future<Void> send(String destination, Map<String, String> headers, Buffer body);

    /**
     * Sends a SUBSCRIBE frame and routes every MESSAGE frame carrying the subscription's id to the handler.
     *
     * @param id             the subscription id, unique on this connection
     * @param destination    the destination to subscribe to
     * @param headers        the other headers of the frame, may be empty
     * @param messageHandler receives the MESSAGE frames of the subscription
     * @return a future completed when the frame is written, or when the RECEIPT for the frame's {@code receipt}
     *         header arrives; failed with the ERROR frame's {@link StompErrorException}, when the connection
     *         closes first, or at once when the subscription id is in use or another frame is still waiting for
     *         a RECEIPT of the same id
     */
    Future<Void> subscribe(String id, String destination, Map<String, String> headers, Handler<Frame> messageHandler);

    /**
     * Sends an UNSUBSCRIBE frame and stops routing messages to the subscription's handler.
     *
     * @param id the subscription id
     * @return a future completed when the frame is written
     */
    Future<Void> unsubscribe(String id);

    /**
     * Sends a DISCONNECT frame with a receipt and closes the connection once the server answers with the RECEIPT
     * or closes the connection itself.
     *
     * @return a future completed when the connection is closed
     */
    Future<Void> disconnect();

    /**
     * @return true while the connection is open
     */
    boolean isConnected();

    /**
     * Closes the WebSocket without a DISCONNECT frame.
     */
    void close();

    /**
     * Sets the handler invoked with every ERROR frame the server sends. An ERROR frame naming a {@code receipt-id}
     * fails the operation waiting on that receipt with the frame's {@link StompErrorException}; one naming none
     * fails every operation waiting on a receipt. The connection closes after an ERROR frame, and the
     * {@link #closeHandler} then receives the same exception as the cause.
     *
     * @param handler the handler
     * @return this for fluent usage
     */
    StompClientConnection errorHandler(Handler<Frame> handler);

    /**
     * Sets the handler invoked once when the connection closes, however it closes. The cause is what ended the
     * connection: the {@link StompErrorException} of an ERROR frame, the heartbeat failure of a silent server, a
     * frame the client could not parse or accept, or a transport failure. It is {@code null} when the connection
     * closed without any such failure, after {@link #disconnect()}, {@link #close()} or a close the server
     * initiated without an ERROR frame. A handler set after the connection closed is invoked at once.
     *
     * @param handler the handler
     * @return this for fluent usage
     */
    StompClientConnection closeHandler(Handler<Throwable> handler);

}
