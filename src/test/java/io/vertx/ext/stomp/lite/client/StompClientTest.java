package io.vertx.ext.stomp.lite.client;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.UpgradeRejectedException;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.stomp.lite.StompServerOptions;
import io.vertx.ext.stomp.lite.StompServerVerticleFactory;
import io.vertx.ext.stomp.lite.frame.Frame;
import io.vertx.ext.stomp.lite.frame.FrameParser;
import io.vertx.ext.stomp.lite.frame.Headers;
import io.vertx.ext.web.handler.HttpException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Runs the client against the library's own server over a real WebSocket.
 */
public class StompClientTest {

    private static final String WEBSOCKET_PATH = "/stomp";
    private static final long TIMEOUT_SECONDS = 10;

    private Vertx vertx;
    private int port;
    private RecordingStompServerHandler handler;
    private StompClient client;

    @Before
    public void setUp() throws Exception {
        vertx = Vertx.vertx();
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (client != null) {
                client.close().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        } finally {
            vertx.close().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    public void connectSendsItsHeadersAndCompletesWithTheConnectedFrame() throws Exception {
        startServer(headers -> Future.succeededFuture(Map.of("welcome", headers.get(Frame.LOGIN))), new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions());

        StompClientConnection connection = connect(Map.of(Frame.LOGIN, "alice", Frame.PASSCODE, "secret"));

        assertTrue(connection.isConnected());
        assertEquals("alice", connection.connectedFrame().getHeader("welcome"));
        assertEquals("1.2", connection.connectedFrame().getHeader(Frame.VERSION));
        assertNotNull(connection.connectedFrame().getHeader(Frame.HEARTBEAT));
    }

    @Test
    public void connectFailsWithTheStatusOfTheHttpExceptionTheHandshakeFailsWith() throws Exception {
        // a status other than 401, so a server answering every failure with a 401 would fail the test
        startServer(() -> Future.failedFuture(new HttpException(503, "the authentication service is unavailable")),
                    headers -> Future.succeededFuture(Collections.emptyMap()),
                    new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions());

        try {
            connect(Map.of(Frame.LOGIN, "alice"));
            fail("connect should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof UpgradeRejectedException);
            assertEquals(503, ((UpgradeRejectedException) e.getCause()).getStatus());
        }
    }

    @Test
    public void connectFailsWithA500WhenTheHandshakeFailsWithAnyOtherException() throws Exception {
        startServer(() -> Future.failedFuture(new IllegalStateException("a failure the handler did not classify")),
                    headers -> Future.succeededFuture(Collections.emptyMap()),
                    new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions());

        try {
            connect(Map.of(Frame.LOGIN, "alice"));
            fail("connect should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof UpgradeRejectedException);
            assertEquals(500, ((UpgradeRejectedException) e.getCause()).getStatus());
        }
    }

    @Test
    public void connectFailsWithTheErrorFrameTheServerAnswers() throws Exception {
        startServer(headers -> Future.failedFuture(new IllegalArgumentException("bad credentials")), new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions());

        try {
            connect(Map.of(Frame.LOGIN, "alice"));
            fail("connect should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StompErrorException);
            StompErrorException error = (StompErrorException) e.getCause();
            assertEquals("bad credentials", error.getMessage());
            assertEquals(Frame.Command.ERROR, error.getFrame().getCommand());
        }
    }

    @Test
    public void subscribeThenReceive() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions());
        StompClientConnection connection = connect(Collections.emptyMap());
        CompletableFuture<Frame> received = new CompletableFuture<>();

        // with a receipt the subscription completes once the server has processed it
        connection.subscribe("sub-1", "/queue/greetings", Map.of(Frame.RECEIPT, "s-1"), received::complete).await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        Frame subscribe = handler.subscribeFrames().poll();
        assertNotNull(subscribe);
        assertEquals("sub-1", subscribe.getId());
        assertEquals("/queue/greetings", subscribe.getDestination());
        assertEquals("s-1", subscribe.getReceipt());

        handler.message("sub-1", "/queue/greetings", Buffer.buffer("hello"));

        Frame message = received.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("/queue/greetings", message.getDestination());
        assertEquals("hello", message.getBodyAsString());

        connection.unsubscribe("sub-1").await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Frame unsubscribe = handler.unsubscribeFrames().poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(unsubscribe);
        assertEquals("sub-1", unsubscribe.getId());
    }

    @Test
    public void sendDeliversTheFrameAndWaitsForItsReceipt() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions());
        StompClientConnection connection = connect(Collections.emptyMap());
        // a body with a NULL byte, which only survives with a content-length
        Buffer body = Buffer.buffer(new byte[]{1, 0, 2});

        connection.send("/queue/orders", Map.of(Frame.RECEIPT, "r-1", "custom", "value"), body)
                  .await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        Frame send = handler.sendFrames().poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(send);
        assertEquals("/queue/orders", send.getDestination());
        assertEquals("value", send.getHeader("custom"));
        assertEquals("r-1", send.getReceipt());
        assertEquals("3", send.getHeader(Frame.CONTENT_LENGTH));
        assertArrayEquals(new byte[]{1, 0, 2}, send.getBodyAsByteArray());

        connection.send("/queue/orders", Collections.emptyMap(), null).await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Frame bodiless = handler.sendFrames().poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(bodiless);
        assertTrue(bodiless.hasEmptyBody());
    }

    @Test
    public void errorFrameFailsTheOperationItNames() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions());
        StompClientConnection connection = connect(Collections.emptyMap());
        CompletableFuture<Throwable> closeCause = new CompletableFuture<>();
        connection.closeHandler(closeCause::complete);

        Future<Void> refused = connection.send(RecordingStompServerHandler.REFUSED_DESTINATION, Map.of(Frame.RECEIPT, "r-1"), null);
        Future<Void> other = connection.send("/queue/orders", Map.of(Frame.RECEIPT, "r-2"), null);

        try {
            refused.toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            fail("the refused send should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StompErrorException);
            StompErrorException error = (StompErrorException) e.getCause();
            assertEquals("refused", error.getMessage());
            assertEquals("r-1", error.getFrame().getHeader(Frame.RECEIPT_ID));
        }
        // the other send fails with the close, never with the refusal, and the server did not process it
        try {
            other.toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            fail("the other send should fail with the close");
        } catch (ExecutionException e) {
            assertFalse(e.getCause() instanceof StompErrorException);
            assertTrue(e.getCause().getCause() instanceof StompErrorException);
        }
        assertTrue(closeCause.get(TIMEOUT_SECONDS, TimeUnit.SECONDS) instanceof StompErrorException);
        assertFalse(connection.isConnected());
        assertTrue(handler.sendFrames().isEmpty());
    }

    @Test
    public void sendOverTheDefaultWebSocketFrameSizeReachesTheServer() throws Exception {
        // the verticle accepts WebSocket frames of Vert.x's default size only
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions());
        StompClientConnection connection = connect(Collections.emptyMap());
        byte[] body = new byte[200_000];
        body[body.length - 1] = 7;

        connection.send("/queue/big", Map.of(Frame.RECEIPT, "r-1"), Buffer.buffer(body)).await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        Frame send = handler.sendFrames().poll();
        assertNotNull(send);
        assertArrayEquals(body, send.getBodyAsByteArray());
    }

    @Test
    public void malformedContentLengthEndsTheConnection() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        // a raw client, since the client never writes such a frame
        LinkedBlockingQueue<Frame> frames = new LinkedBlockingQueue<>();
        CompletableFuture<Void> closed = new CompletableFuture<>();
        WebSocket socket = openRawSocket(frames::add, closed);
        socket.writeBinaryMessage(new Frame(Frame.Command.CONNECT,
                                            Headers.create(Frame.ACCEPT_VERSION, "1.2", Frame.HOST, "localhost"),
                                            null).toBuffer());
        assertEquals(Frame.Command.CONNECTED, frames.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS).getCommand());

        // a frame the server cannot parse ends the connection instead of leaving the parser mid-frame
        socket.writeBinaryMessage(Buffer.buffer("SEND\ndestination:/queue/orders\ncontent-length:abc\n\nhello\0"));

        closed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertTrue(handler.sendFrames().isEmpty());
    }

    @Test
    public void invalidClientHeartbeatIsAnsweredWithAnError() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        LinkedBlockingQueue<Frame> frames = new LinkedBlockingQueue<>();
        CompletableFuture<Void> closed = new CompletableFuture<>();
        WebSocket socket = openRawSocket(frames::add, closed);

        socket.writeBinaryMessage(new Frame(Frame.Command.CONNECT,
                                            Headers.create(Frame.ACCEPT_VERSION, "1.2", Frame.HOST, "localhost", Frame.HEARTBEAT, "soon"),
                                            null).toBuffer());

        Frame answer = frames.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(answer);
        assertEquals(Frame.Command.ERROR, answer.getCommand());
        assertTrue(answer.getHeader(Frame.MESSAGE), answer.getHeader(Frame.MESSAGE).contains("heart-beat"));
        closed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void outgoingHeartbeatKeepsTheConnectionAlive() throws Exception {
        // the server wants a heartbeat every 200 ms and drops a client silent for two of them
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()),
                    new StompServerOptions().setHeartbeat(new JsonObject().put("x", 0).put("y", 200)));
        client = StompClient.create(vertx, new StompClientOptions().setHeartbeat(new JsonObject().put("x", 200).put("y", 0)));
        StompClientConnection connection = connect(Collections.emptyMap());
        CompletableFuture<Throwable> closeCause = new CompletableFuture<>();
        connection.closeHandler(closeCause::complete);

        Thread.sleep(1500);

        assertFalse(closeCause.isDone());
        assertTrue(connection.isConnected());
    }

    @Test
    public void errorHandlerRunsBeforeTheCloseHandler() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions());
        StompClientConnection connection = connect(Collections.emptyMap());
        List<String> order = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> closed = new CompletableFuture<>();
        connection.errorHandler(frame -> order.add("error " + frame.getHeader(Frame.RECEIPT_ID)));
        connection.closeHandler(cause -> {
            order.add("close");
            closed.complete(null);
        });

        connection.send(RecordingStompServerHandler.REFUSED_DESTINATION, Map.of(Frame.RECEIPT, "r-1"), null);

        closed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(List.of("error r-1", "close"), order);
    }

    @Test
    public void closingTheClientEndsItsConnections() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions());
        StompClientConnection connection = connect(Collections.emptyMap());
        CompletableFuture<Throwable> closeCause = new CompletableFuture<>();
        connection.closeHandler(closeCause::complete);

        client.close().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNull(closeCause.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertFalse(connection.isConnected());
    }

    @Test
    public void callerNamingNoSubProtocolConnectsToAServerEchoingNone() throws Exception {
        startScriptedServer(frame -> frame.getCommand() == Frame.Command.CONNECT
                ? new Frame(Frame.Command.CONNECTED, Headers.create(Frame.VERSION, "1.2", Frame.HEARTBEAT, "0,0"), null)
                : null, false);
        client = StompClient.create(vertx, new StompClientOptions());
        WebSocketConnectOptions options = new WebSocketConnectOptions().setHost("localhost")
                                                                       .setPort(port)
                                                                       .setURI(WEBSOCKET_PATH)
                                                                       .setSubProtocols(List.of());

        StompClientConnection connection = client.connect(options, Collections.emptyMap())
                                                 .toCompletionStage()
                                                 .toCompletableFuture()
                                                 .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue(connection.isConnected());
    }

    @Test
    public void disconnectClosesWhenTheServerAnswersNothing() throws Exception {
        // a server that accepts the connection and ignores the DISCONNECT frame
        startScriptedServer(frame -> frame.getCommand() == Frame.Command.CONNECT
                ? new Frame(Frame.Command.CONNECTED, Headers.create(Frame.VERSION, "1.2", Frame.HEARTBEAT, "0,0"), null)
                : null);
        client = StompClient.create(vertx, new StompClientOptions().setConnectTimeout(300));
        StompClientConnection connection = connect(Collections.emptyMap());
        CompletableFuture<Throwable> closeCause = new CompletableFuture<>();
        connection.closeHandler(closeCause::complete);

        long started = System.nanoTime();
        connection.disconnect().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 5000);
        assertFalse(connection.isConnected());
        // the unanswered DISCONNECT is the cause of the close, where an answered one closes with none
        Throwable cause = closeCause.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertTrue(String.valueOf(cause), cause.getMessage().contains("DISCONNECT"));
    }

    @Test
    public void invalidHeartbeatHeaderFailsTheConnect() throws Exception {
        startScriptedServer(frame -> frame.getCommand() == Frame.Command.CONNECT
                ? new Frame(Frame.Command.CONNECTED, Headers.create(Frame.VERSION, "1.2", Frame.HEARTBEAT, "soon"), null)
                : null);
        client = StompClient.create(vertx, new StompClientOptions());

        try {
            connect(Collections.emptyMap());
            fail("the connect should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause().getMessage(), e.getCause().getMessage().contains("heart-beat"));
        }
    }

    @Test
    public void silentServerIsLostAfterTheNegotiatedHeartbeat() throws Exception {
        // a scripted server that promises a heartbeat every 300 ms, with the space some servers write, and then
        // says nothing
        startScriptedServer(frame -> frame.getCommand() == Frame.Command.CONNECT
                ? new Frame(Frame.Command.CONNECTED, Headers.create(Frame.VERSION, "1.2", Frame.HEARTBEAT, "300, 0"), null)
                : null);
        client = StompClient.create(vertx, new StompClientOptions().setHeartbeat(new JsonObject().put("x", 0).put("y", 300)));
        StompClientConnection connection = connect(Collections.emptyMap());
        CompletableFuture<Throwable> closeCause = new CompletableFuture<>();
        connection.closeHandler(closeCause::complete);

        long started = System.nanoTime();
        Throwable cause = closeCause.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(cause);
        assertTrue(cause.getMessage(), cause.getMessage().contains("heartbeat"));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 5000);
        assertFalse(connection.isConnected());
    }

    @Test
    public void serverSilentAfterTheUpgradeFailsTheConnectWithinTheTimeout() throws Exception {
        // a scripted server that opens the WebSocket and never answers the CONNECT frame
        CompletableFuture<Void> socketClosed = new CompletableFuture<>();
        HttpServer server = vertx.createHttpServer(new HttpServerOptions().setPort(port).setWebSocketSubProtocols(List.of(Frame.WEBSOCKET_SUB_PROTOCOL)))
                                 .webSocketHandler(socket -> {
                                     socket.handler(buffer -> {});
                                     socket.closeHandler(v -> socketClosed.complete(null));
                                 });
        server.listen().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        client = StompClient.create(vertx, new StompClientOptions().setConnectTimeout(300));

        long started = System.nanoTime();
        try {
            connect(Collections.emptyMap());
            fail("the connect should time out");
        } catch (ExecutionException e) {
            assertTrue(e.getCause().getMessage(), e.getCause().getMessage().contains("CONNECT"));
        }
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 5000);
        // the socket the server opened does not outlive the failed connect
        socketClosed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void frameOverTheSizeLimitEndsTheConnection() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions().setMaxBodyLength(1024));
        StompClientConnection connection = connect(Collections.emptyMap());
        CompletableFuture<Frame> received = new CompletableFuture<>();
        CompletableFuture<Throwable> closeCause = new CompletableFuture<>();
        connection.closeHandler(closeCause::complete);
        connection.subscribe("sub-1", "/queue/big", Map.of(Frame.RECEIPT, "s-1"), received::complete).await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        handler.message("sub-1", "/queue/big", Buffer.buffer(new byte[4096]));

        assertNotNull(closeCause.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertFalse(received.isDone());
        assertFalse(connection.isConnected());
    }

    @Test
    public void disconnectReceivesTheReceiptAndCloses() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions());
        StompClientConnection connection = connect(Collections.emptyMap());
        CompletableFuture<Throwable> closeCause = new CompletableFuture<>();
        connection.closeHandler(closeCause::complete);

        connection.disconnect().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        handler.disconnectedSignal().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        handler.closedSignal().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNull(closeCause.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertFalse(connection.isConnected());
        // a handler set after the close learns of it all the same
        CompletableFuture<Throwable> lateCloseCause = new CompletableFuture<>();
        connection.closeHandler(lateCloseCause::complete);
        assertNull(lateCloseCause.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    public void serverStreamingAnUnterminatedHeaderEndsTheConnection() throws Exception {
        // a server that answers CONNECT and then writes a header line that never ends, in small messages
        HttpServerOptions httpOptions = new HttpServerOptions().setPort(port)
                                                               .setWebSocketSubProtocols(List.of(Frame.WEBSOCKET_SUB_PROTOCOL));
        vertx.createHttpServer(httpOptions)
             .webSocketHandler(socket -> {
                 FrameParser parser = new FrameParser();
                 parser.handler(frame -> {
                     socket.writeBinaryMessage(new Frame(Frame.Command.CONNECTED, Headers.create(Frame.VERSION, "1.2"), null).toBuffer());
                     socket.writeBinaryMessage(Buffer.buffer("MESSAGE\ndestination:"));
                     Buffer piece = Buffer.buffer("x".repeat(1024));
                     for (int i = 0; i < 100; i++) {
                         socket.writeBinaryMessage(piece);
                     }
                 });
                 socket.handler(parser);
             })
             .listen().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        client = StompClient.create(vertx, new StompClientOptions());
        StompClientConnection connection = connect(Collections.emptyMap());
        CompletableFuture<Throwable> closeCause = new CompletableFuture<>();
        connection.closeHandler(closeCause::complete);

        Throwable cause = closeCause.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue(String.valueOf(cause), cause.getMessage().contains("Header length exceeded"));
        assertFalse(connection.isConnected());
    }

    @Test
    public void clientStreamingAnUnterminatedHeaderIsDisconnected() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        LinkedBlockingQueue<Frame> frames = new LinkedBlockingQueue<>();
        CompletableFuture<Void> closed = new CompletableFuture<>();
        WebSocket socket = openRawSocket(frames::add, closed);
        socket.writeBinaryMessage(new Frame(Frame.Command.CONNECT,
                                            Headers.create(Frame.ACCEPT_VERSION, "1.2", Frame.HOST, "localhost"),
                                            null).toBuffer());
        assertEquals(Frame.Command.CONNECTED, frames.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS).getCommand());
        // a header value of the longest length the server allows still gets through
        String longest = "v".repeat(StompServerOptions.DEFAULT_MAX_HEADER_LENGTH);
        socket.writeBinaryMessage(Buffer.buffer("SEND\ndestination:/queue/orders\nnote:" + longest + "\n\n\0"));
        assertEquals(longest, handler.sendFrames().poll(TIMEOUT_SECONDS, TimeUnit.SECONDS).getHeader("note"));

        // a header line that never ends is cut off once it passes the limit instead of buffering without bound
        socket.writeBinaryMessage(Buffer.buffer("SEND\ndestination:"));
        Buffer piece = Buffer.buffer("x".repeat(1024));
        for (int i = 0; i < 100; i++) {
            socket.writeBinaryMessage(piece);
        }

        closed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertTrue(handler.sendFrames().isEmpty());
    }

    @Test
    public void secondConnectWhileTheFirstIsPendingIsRejected() throws Exception {
        // the handler holds the first CONNECT open until the test releases it
        AtomicInteger connectCalls = new AtomicInteger();
        CompletableFuture<Runnable> releaseFirstConnect = new CompletableFuture<>();
        startServer(headers -> {
            connectCalls.incrementAndGet();
            Context context = Vertx.currentContext();
            Promise<Map<String, String>> pending = Promise.promise();
            releaseFirstConnect.complete(() -> context.runOnContext(v -> pending.tryComplete(Collections.emptyMap())));
            return pending.future();
        }, new StompServerOptions().setHeartbeat(new JsonObject().put("x", 100).put("y", 100)));
        LinkedBlockingQueue<Frame> frames = new LinkedBlockingQueue<>();
        CompletableFuture<Void> closed = new CompletableFuture<>();
        WebSocket socket = openRawSocket(frames::add, closed);
        Buffer connect = new Frame(Frame.Command.CONNECT,
                                   Headers.create(Frame.ACCEPT_VERSION, "1.2", Frame.HOST, "localhost", Frame.HEARTBEAT, "100,100"),
                                   null).toBuffer();

        socket.writeBinaryMessage(connect);
        Runnable release = releaseFirstConnect.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        socket.writeBinaryMessage(connect);

        // the second CONNECT is answered with an ERROR and the connection closed, without reaching the handler
        Frame error = frames.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(error);
        assertEquals(Frame.Command.ERROR, error.getCommand());
        closed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        handler.closedSignal().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(1, connectCalls.get());

        // the first CONNECT completing after the close neither answers the client nor starts heartbeats
        release.run();
        assertNull(frames.poll(500, TimeUnit.MILLISECONDS));
        assertEquals(1, connectCalls.get());
    }

    @Test
    public void connectAfterConnectedIsRejected() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        LinkedBlockingQueue<Frame> frames = new LinkedBlockingQueue<>();
        CompletableFuture<Void> closed = new CompletableFuture<>();
        WebSocket socket = openRawSocket(frames::add, closed);
        Buffer connect = new Frame(Frame.Command.CONNECT,
                                   Headers.create(Frame.ACCEPT_VERSION, "1.2", Frame.HOST, "localhost"),
                                   null).toBuffer();

        socket.writeBinaryMessage(connect);
        assertEquals(Frame.Command.CONNECTED, frames.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS).getCommand());
        socket.writeBinaryMessage(connect);

        assertEquals(Frame.Command.ERROR, frames.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS).getCommand());
        closed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void frameWrittenWholeIsAcceptedUpToTheBodyLimit() throws Exception {
        // a scripted server answers SUBSCRIBE with a MESSAGE whose body is just under the limit, written in one
        // WebSocket frame that the headers push past the body limit; the WebSocket frame size is the transport's own,
        // so only the body is measured against the limit
        Buffer body = Buffer.buffer(new byte[1023]);
        startScriptedServer(frame -> switch (frame.getCommand()) {
            case CONNECT -> new Frame(Frame.Command.CONNECTED, Headers.create(Frame.VERSION, "1.2"), null);
            case SUBSCRIBE -> new Frame(Frame.Command.MESSAGE,
                                        Headers.create(Frame.SUBSCRIPTION, frame.getHeader(Frame.ID),
                                                       Frame.DESTINATION, frame.getDestination(),
                                                       Frame.MESSAGE_ID, "m-1",
                                                       Frame.CONTENT_LENGTH, Integer.toString(body.length())),
                                        body);
            default -> null;
        });
        client = StompClient.create(vertx, new StompClientOptions().setMaxBodyLength(1024));
        StompClientConnection connection = connect(Collections.emptyMap());
        CompletableFuture<Frame> received = new CompletableFuture<>();
        connection.subscribe("sub-1", "/queue/big", Collections.emptyMap(), received::complete).await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertEquals(body.length(), received.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getBody().length());
        assertTrue(connection.isConnected());
    }

    @Test
    public void clientHeartbeatIsABareEndOfLineOnTheWire() throws Exception {
        // a server that records every WebSocket frame the client writes and wants a heartbeat every 200 ms
        LinkedBlockingQueue<Buffer> written = new LinkedBlockingQueue<>();
        HttpServerOptions httpOptions = new HttpServerOptions().setPort(port)
                                                               .setWebSocketSubProtocols(List.of(Frame.WEBSOCKET_SUB_PROTOCOL));
        vertx.createHttpServer(httpOptions)
             .webSocketHandler(socket -> {
                 FrameParser parser = new FrameParser();
                 parser.handler(frame -> {
                     if (frame.getCommand() == Frame.Command.CONNECT) {
                         socket.writeBinaryMessage(new Frame(Frame.Command.CONNECTED,
                                                             Headers.create(Frame.VERSION, "1.2", Frame.HEARTBEAT, "0,200"),
                                                             null).toBuffer());
                     }
                 });
                 socket.handler(buffer -> {
                     written.add(buffer.copy());
                     parser.handle(buffer);
                 });
             })
             .listen().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        client = StompClient.create(vertx, new StompClientOptions().setHeartbeat(new JsonObject().put("x", 200).put("y", 0)));
        connect(Collections.emptyMap());

        // the CONNECT frame first, then the heartbeat STOMP 1.2 defines: a single end-of-line
        Buffer first = written.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("CONNECT", first.getString(0, "CONNECT".length()));
        Buffer heartbeat = written.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(FrameParser.EOL, heartbeat.toString());
    }

    @Test
    public void serverHeartbeatIsABareEndOfLineOnTheWire() throws Exception {
        // the server offers a heartbeat every 200 ms and a raw client asking for one records every frame it receives
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()),
                    new StompServerOptions().setHeartbeat(new JsonObject().put("x", 200).put("y", 0)));
        LinkedBlockingQueue<Buffer> received = new LinkedBlockingQueue<>();
        WebSocket socket = vertx.createWebSocketClient()
                                .connect(new WebSocketConnectOptions().setHost("localhost")
                                                                      .setPort(port)
                                                                      .setURI(WEBSOCKET_PATH)
                                                                      .addSubProtocol(Frame.WEBSOCKET_SUB_PROTOCOL))
                                .await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        socket.handler(buffer -> received.add(buffer.copy()));
        socket.writeBinaryMessage(new Frame(Frame.Command.CONNECT,
                                            Headers.create(Frame.ACCEPT_VERSION, "1.2", Frame.HOST, "localhost", Frame.HEARTBEAT, "0,200"),
                                            null).toBuffer());

        // the CONNECTED frame first, then the heartbeat STOMP 1.2 defines: a single end-of-line
        Buffer first = received.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("CONNECTED", first.getString(0, "CONNECTED".length()));
        Buffer heartbeat = received.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(FrameParser.EOL, heartbeat.toString());
    }

    private void startServer(Function<Map<String, String>, Future<Map<String, String>>> connectAnswer,
                             StompServerOptions stompOptions) throws Exception {
        startServer(() -> Future.succeededFuture(MultiMap.caseInsensitiveMultiMap()), connectAnswer, stompOptions);
    }

    private void startServer(Supplier<Future<MultiMap>> handshakeAnswer,
                             Function<Map<String, String>, Future<Map<String, String>>> connectAnswer,
                             StompServerOptions stompOptions) throws Exception {
        handler = new RecordingStompServerHandler(handshakeAnswer, connectAnswer);
        // the verticle accepts the STOMP sub-protocol on its own, so the options name none
        HttpServerOptions httpOptions = new HttpServerOptions().setPort(port);
        vertx.deployVerticle(StompServerVerticleFactory.create(httpOptions,
                                                               stompOptions.setWebsocketPath(WEBSOCKET_PATH),
                                                               () -> handler,
                                                               null))
             .await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // a server that answers each frame with what the script returns, nothing for null, and never closes on its own
    private void startScriptedServer(Function<Frame, Frame> script) throws Exception {
        startScriptedServer(script, true);
    }

    private void startScriptedServer(Function<Frame, Frame> script, boolean echoingTheSubProtocol) throws Exception {
        HttpServerOptions httpOptions = new HttpServerOptions().setPort(port);
        if (echoingTheSubProtocol) {
            httpOptions.setWebSocketSubProtocols(List.of(Frame.WEBSOCKET_SUB_PROTOCOL));
        }
        HttpServer server = vertx.createHttpServer(httpOptions)
                                 .webSocketHandler(socket -> {
                                     FrameParser parser = new FrameParser();
                                     parser.handler(frame -> {
                                         Frame answer = script.apply(frame);
                                         if (answer != null) {
                                             socket.writeBinaryMessage(answer.toBuffer());
                                         }
                                     });
                                     socket.handler(parser);
                                 });
        server.listen().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // a WebSocket to the server under the STOMP sub-protocol, whose frames go to the handler
    private WebSocket openRawSocket(Handler<Frame> frames, CompletableFuture<Void> closed) throws Exception {
        WebSocketClient rawClient = vertx.createWebSocketClient();
        WebSocket socket = rawClient.connect(new WebSocketConnectOptions().setHost("localhost")
                                                                         .setPort(port)
                                                                         .setURI(WEBSOCKET_PATH)
                                                                         .addSubProtocol(Frame.WEBSOCKET_SUB_PROTOCOL))
                                    .await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        socket.closeHandler(v -> closed.complete(null));
        FrameParser parser = new FrameParser();
        parser.handler(frames);
        socket.handler(parser);
        return socket;
    }

    private StompClientConnection connect(Map<String, String> connectHeaders) throws Exception {
        WebSocketConnectOptions options = new WebSocketConnectOptions().setHost("localhost").setPort(port).setURI(WEBSOCKET_PATH);
        return client.connect(options, connectHeaders).toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

}
