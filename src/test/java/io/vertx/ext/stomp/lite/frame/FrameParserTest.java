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
    private static final int MAX_BODY_LENGTH = 32;

    private final List<Frame> frames = new ArrayList<>();
    private final List<FrameException> errors = new ArrayList<>();
    private final FrameParser parser = new FrameParser(new StompServerOptions().setMaxHeaderLength(MAX_HEADER_LENGTH)
                                                                               .setMaxBodyLength(MAX_BODY_LENGTH))
            .handler(frames::add)
            .errorHandler(errors::add);

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
    public void unterminatedBodyIsReportedOncePastTheLimit() {
        // headers and the start of the body in one chunk, with no terminator in sight
        parser.handle(Buffer.buffer("SEND\ndestination:/q\n\n" + "y".repeat(MAX_BODY_LENGTH + 1)));

        assertEquals(1, errors.size());
        assertEquals("Body size exceeded", errors.get(0).getMessage());
        assertTrue(frames.isEmpty());
    }
}
