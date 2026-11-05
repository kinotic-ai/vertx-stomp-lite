package io.vertx.ext.stomp.lite;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.stomp.lite.frame.Frame;
import javax.net.ssl.SSLSession;

/**
 *
 * Created by Navid Mitchell on 2019-01-10.
 */
public interface StompServerConnection {

    /**
     * When a {@code Websocket} is created it automatically registers an event handler with the event bus - the ID of that
     * handler is given by this method.
     *
     * Given this ID, a different event loop can send a binary frame to that event handler using the event bus and
     * that buffer will be received by this instance in its own event loop and written to the underlying connection. This
     * allows you to write data to other WebSockets which are owned by different event loops.
     *
     * @return the binary handler id
     */
    String binaryHandlerID();

    /**
     * When a {@code Websocket} is created it automatically registers an event handler with the eventbus, the ID of that
     * handler is given by {@code textHandlerID}.
     *
     * Given this ID, a different event loop can send a text frame to that event handler using the event bus and
     * that buffer will be received by this instance in its own event loop and written to the underlying connection. This
     * allows you to write data to other WebSockets which are owned by different event loops.
     *
     * @return the text handler id
     */
    String textHandlerID();

    /**
     * @return the remote address for this socket
     */
    SocketAddress remoteAddress();

    /**
     * @return the local address for this socket
     */
    SocketAddress localAddress();

    /**
     * @return true if this {@link io.vertx.core.http.HttpConnection} is encrypted via SSL/TLS.
     */
    boolean isSsl();

    /**
     * @return SSLSession associated with the underlying socket. Returns null if connection is
     *         not SSL.
     * @see javax.net.ssl.SSLSession
     */
    SSLSession sslSession();

    /**
     * Writes the given frame to the socket.
     *
     * @param frame the frame, must not be {@code null}.
     * @return a future completed when the data is successfully sent.
     *         Will be failed if there is a problem sending the data or the underlying TCP connection is already closed.
     */
    Future<Void> write(Frame frame);

    /**
     * Writes the given buffer to the socket. This is a low level API that should be used carefully.
     *
     * @param buffer the buffer
     * @return a future completed when the data is successfully sent.
     *         Will be failed if there is a problem sending the data or the underlying TCP connection is already closed.
     */
    Future<Void> write(Buffer buffer);

    /**
     * Will send receipt frame acknowledgement to clients when requested by a receipt header
     * @param frame to check for a receipt header
     * @return a future completed when the data is successfully sent.
     *         Will be failed if there is a problem sending the data or the underlying TCP connection is already closed.
     */
    Future<Void> sendReceiptIfNeeded(Frame frame);

    /**
     * Sends an error frame to the client and leaves the client connected
     * @param throwable the error that occurred
     * @return a future completed when the data is successfully sent.
     *         Will be failed if there is a problem sending the data or the underlying TCP connection is already closed.
     * @deprecated does not comply to the STOMP spec
     */
    @Deprecated(since = "3.9.17")
    Future<Void> sendError(Throwable throwable);

    /**
     * Sends an error frame to the client and then disconnects the client per the Stomp Spec
     * @param throwable the error that occurred
     * @return a future completed when the data is successfully sent.
     *         Will be failed if there is a problem sending the data or the underlying TCP connection is already closed.
     */
    Future<Void> sendErrorAndDisconnect(Throwable throwable);

    /**
     * Sends an error frame naming the client frame the server could not process, then disconnects the client per
     * the Stomp Spec: the ERROR frame carries the {@code receipt-id} of that frame's {@code receipt} header, so the
     * client knows which of its frames failed.
     * @param throwable the error that occurred
     * @param causingFrame the client frame the server could not process, or null when no frame is to blame
     * @return a future completed when the data is successfully sent.
     *         Will be failed if there is a problem sending the data or the underlying TCP connection is already closed.
     */
    Future<Void> sendErrorAndDisconnect(Throwable throwable, Frame causingFrame);

    /**
     * Pause the client from sending data. it sets the buffer in {@code fetch} mode and clears the actual demand.
     *
     * While it's paused, no data will be sent to the data {@link StompServerHandler}.
     */
    void pause();

    /**
     * Resume reading, and sets the buffer in {@code flowing} mode.
     *
     * If this has been paused, data receiving recommence on it.
     */
    void resume();

    /**
     * Fetch the specified {@code amount} of elements. If this has been paused, reading will
     * recommence with the specified {@code amount} of items, otherwise the specified {@code amount} will
     * be added to the current client demand.
     *
     * @param amount to fetch
     */
    void fetch(long amount);

    /**
     * Closes the connection with the client.
     */
    void close();

}
