package io.vertx.ext.stomp.lite.frame;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.lite.StompServerOptions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Feeds the parser raw bytes and checks the limits it enforces on a frame that is still incomplete.
 */
public class FrameParserTest {

    private static final int MAX_HEADER_LENGTH = 16;
    private static final int MAX_HEADERS = 3;
    private static final int MAX_BODY_LENGTH = 32;

    private final List<Frame> frames = new ArrayList<>();
    private final List<FrameException> errors = new ArrayList<>();
    private final StompServerOptions options = new StompServerOptions().setMaxHeaderLength(MAX_HEADER_LENGTH)
                                                                       .setMaxHeaders(MAX_HEADERS)
                                                                       .setMaxBodyLength(MAX_BODY_LENGTH);
    private final FrameParser parser = new FrameParser(options).handler(frames::add)
                                                               .errorHandler(errors::add);

    @Test
    public void largestFrameTheLimitsAllowFitsMaxFrameLength() {
        // every header at the limit in three-byte chars, the body one byte under its limit
        StringBuilder frame = new StringBuilder("SEND\n");
        for (int i = 0; i < MAX_HEADERS; i++) {
            frame.append("€".repeat(MAX_HEADER_LENGTH - 1)).append(i).append(':').append("€".repeat(MAX_HEADER_LENGTH)).append("\r\n");
        }
        frame.append('\n').append("b".repeat(MAX_BODY_LENGTH - 1)).append('\0');
        Buffer bytes = Buffer.buffer(frame.toString());

        parser.handle(bytes);

        assertTrue(bytes.length() + " > " + FrameParser.maxFrameLength(options), bytes.length() <= FrameParser.maxFrameLength(options));
        assertTrue(errors.toString(), errors.isEmpty());
        assertEquals(1, frames.size());
        assertEquals(MAX_HEADERS, frames.get(0).getHeaders().size());
    }

    @Test
    public void headerLineOfTheLongestAllowedNameAndValueParses() {
        // three-byte chars, the widest a UTF-16 char encodes to
        String name = "€".repeat(MAX_HEADER_LENGTH);
        String value = "€".repeat(MAX_HEADER_LENGTH);

        parser.handle(Buffer.buffer("SEND\n" + name + ":" + value + "\r\n\n\0"));

        assertTrue(errors.toString(), errors.isEmpty());
        assertEquals(1, frames.size());
        assertEquals(value, frames.get(0).getHeader(name));
    }

    @Test
    public void unterminatedHeaderLineIsReportedOncePastTheLimit() {
        parser.handle(Buffer.buffer("SEND\n"));
        parser.handle(Buffer.buffer("x".repeat(6 * MAX_HEADER_LENGTH + 2)));
        assertTrue(errors.toString(), errors.isEmpty());

        parser.handle(Buffer.buffer("x"));

        assertEquals(1, errors.size());
        assertEquals("Header length exceeded", errors.get(0).getMessage());
        assertTrue(frames.isEmpty());
    }

    @Test
    public void declaredBodyLengthOverTheLimitIsReportedBeforeTheBody() {
        parser.handle(Buffer.buffer("SEND\ndestination:/q\ncontent-length:" + MAX_BODY_LENGTH + "\n\n"));

        assertEquals(1, errors.size());
        assertEquals("Body size exceeded", errors.get(0).getMessage());
    }

    @Test
    public void bodyIsMeasuredWithoutTheTerminatorOrTheFrameBehindIt() {
        String body = "b".repeat(MAX_BODY_LENGTH - 1);
        String first = "SEND\ndestination:/q\ncontent-length:" + body.length() + "\n\n" + body + "\0";
        String second = "SEND\ndestination:/q\n\n\0";
        // the body is split across two chunks and the second chunk also carries the next frame
        int split = first.indexOf(body) + 10;
        parser.handle(Buffer.buffer(first.substring(0, split)));
        parser.handle(Buffer.buffer(first.substring(split) + second));

        assertTrue(errors.toString(), errors.isEmpty());
        assertEquals(2, frames.size());
        assertEquals(body, frames.get(0).getBodyAsString());
        assertEquals(0, frames.get(1).getBody().length());
    }

    @Test
    public void emptyBodyDeclaredWithContentLengthParses() {
        parser.handle(Buffer.buffer("SEND\ndestination:/q\ncontent-length:0\n\n\0"));

        assertTrue(errors.toString(), errors.isEmpty());
        assertEquals(1, frames.size());
        assertEquals(0, frames.get(0).getBody().length());
    }

    @Test
    public void bodyDeclaredEmptyButNotEmptyIsReported() {
        parser.handle(Buffer.buffer("SEND\ndestination:/q\ncontent-length:0\n\nhello\0"));

        assertEquals(1, errors.size());
        assertEquals("Body does not match content-length: 0", errors.get(0).getMessage());
        assertTrue(frames.isEmpty());
    }

    @Test
    public void unterminatedBodyIsReportedOncePastTheLimit() {
        // headers and the start of the body in one chunk, with no terminator in sight
        parser.handle(Buffer.buffer("SEND\ndestination:/q\n\n" + "y".repeat(MAX_BODY_LENGTH + 1)));

        assertEquals(1, errors.size());
        assertEquals("Body size exceeded", errors.get(0).getMessage());
        assertTrue(frames.isEmpty());
    }
}
