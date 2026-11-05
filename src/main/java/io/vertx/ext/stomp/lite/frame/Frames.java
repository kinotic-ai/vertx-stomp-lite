/*
 *  Copyright (c) 2011-2015 The original author or authors
 *  ------------------------------------------------------
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *       The Eclipse Public License is available at
 *       http://www.eclipse.org/legal/epl-v10.html
 *
 *       The Apache License v2.0 is available at
 *       http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.ext.stomp.lite.frame;


import io.vertx.core.buffer.Buffer;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Map;
import java.util.Objects;

/**
 * Utility methods to build common {@link Frame}s. It defines a non-STOMP frame ({@code PING}) that is used for
 * heartbeats. When such frame is written on the wire it is just the {@code 0} byte.
 * <p></p>
 * This class is thread-safe.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface Frames {

    Frame PING = new Frame(Frame.Command.PING, Headers.create(), null) {

        @Override
        public Buffer toBuffer() {
            return Buffer.buffer(FrameParser.EOL);
        }
    };

    static Frame createErrorFrame(String message, Map<String, String> headers, String body) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(headers);
        Objects.requireNonNull(body);
        return new Frame(Frame.Command.ERROR,
                         Headers.create(headers)
                                .add(Frame.MESSAGE, message)
                                .add(Frame.CONTENT_LENGTH, Integer.toString(body.length()))
                                .add(Frame.CONTENT_TYPE, "text/plain"),
                         Buffer.buffer(body));
    }

    /**
     * Creates and error frame for given throwable and includes the stack trace in the body if desired
     * @param throwable to create an error frame for
     * @param includeStackTrace true to include the stack trace false to not
     * @return the Frame
     */
    static Frame createErrorFrame(Throwable throwable, boolean includeStackTrace){
        Frame ret;
        String message = throwable.getMessage() != null ? throwable.getMessage() : throwable.getClass().getName();
        if (includeStackTrace) {
            ret = createErrorFrame(message,
                                   Headers.create(),
                                   ExceptionUtils.getStackTrace(throwable));
        } else {
            ret = createErrorFrame(message, Headers.create(), "");
        }
        return ret;
    }

    static Frame createReceiptFrame(String receiptId, Map<String, String> headers) {
        Objects.requireNonNull(receiptId);
        Objects.requireNonNull(headers);
        return new Frame(Frame.Command.RECEIPT,
                         Headers.create(headers)
                                .add(Frame.RECEIPT_ID, receiptId),
                         null);
    }

    static Frame ping() {
        return PING;
    }
}
