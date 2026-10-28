package io.vertx.ext.stomp.lite.client;

import io.vertx.ext.stomp.lite.frame.Frame;

/**
 * The server answered with an ERROR frame. The message is the frame's {@code message} header, and the server
 * closes the connection after an ERROR frame, so every operation pending on the connection fails with it.
 *
 * Created by Navid Mitchell on 2026-10-25.
 */
public class StompErrorException extends RuntimeException {

    private final Frame frame;

    public StompErrorException(Frame frame) {
        super(frame.getHeader(Frame.MESSAGE));
        this.frame = frame;
    }

    /**
     * @return the ERROR frame the server sent
     */
    public Frame getFrame() {
        return frame;
    }

}
