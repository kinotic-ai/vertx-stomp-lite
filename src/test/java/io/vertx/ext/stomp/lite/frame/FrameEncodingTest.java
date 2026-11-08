package io.vertx.ext.stomp.lite.frame;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Checks how a frame reads its encoding from the content-type header.
 */
public class FrameEncodingTest {

    @Test
    public void encodingDefaultsToUtf8() {
        assertEquals("utf-8", encodingOf(null));
        assertEquals("utf-8", encodingOf("text/plain"));
        assertEquals("utf-8", encodingOf(";charset=iso-8859-1")); // no media type before the parameter
        assertEquals("utf-8", encodingOf("text/plain;charset="));
    }

    @Test
    public void encodingIsTheCharsetParameter() {
        assertEquals("iso-8859-1", encodingOf("text/plain;charset=iso-8859-1"));
        assertEquals("UTF-16", encodingOf("text/plain;charset=UTF-16;format=flowed"));
        assertEquals("utf-16", encodingOf("text/plain;charset=iso-8859-1;charset=utf-16"));
        assertEquals("utf-16", encodingOf("text/plain;charset=utf-16;charset="));
        assertEquals("utf-16", encodingOf("text/plain;charset=utf-16;charset=;format=flowed"));
    }

    private static String encodingOf(String contentType) {
        Headers headers = Headers.create();
        if (contentType != null) {
            headers.put(Frame.CONTENT_TYPE, contentType);
        }
        return new Frame(Frame.Command.SEND, headers, null).encoding();
    }

}
