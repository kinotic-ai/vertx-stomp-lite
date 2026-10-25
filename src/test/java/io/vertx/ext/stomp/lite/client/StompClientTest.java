package io.vertx.ext.stomp.lite.client;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.stomp.lite.StompServerOptions;
import io.vertx.ext.stomp.lite.StompServerVerticleFactory;
import io.vertx.ext.stomp.lite.frame.Frame;
import io.vertx.ext.stomp.lite.frame.FrameParser;
import io.vertx.ext.stomp.lite.frame.Headers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
    private String deploymentId;
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
        if (client != null) {
            client.close().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        vertx.close().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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

        connection.subscribe("sub-1", "/queue/greetings", received::complete).await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        Frame subscribe = handler.subscribeFrames().poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(subscribe);
        assertEquals("sub-1", subscribe.getId());
        assertEquals("/queue/greetings", subscribe.getDestination());

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
    public void silentServerIsLostAfterTheNegotiatedHeartbeat() throws Exception {
        // a scripted server that promises a heartbeat every 300 ms and then says nothing
        HttpServer server = vertx.createHttpServer(new HttpServerOptions().setPort(port).setWebSocketSubProtocols(List.of(StompClient.SUB_PROTOCOL)))
                                 .webSocketHandler(socket -> {
                                     FrameParser parser = new FrameParser();
                                     parser.handler(frame -> {
                                         if (frame.getCommand() == Frame.Command.CONNECT) {
                                             socket.writeBinaryMessage(new Frame(Frame.Command.CONNECTED,
                                                                                 Headers.create(Frame.VERSION, "1.2", Frame.HEARTBEAT, "300,0"),
                                                                                 null).toBuffer());
                                         }
                                     });
                                     socket.handler(parser);
                                 });
        server.listen().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
    public void frameOverTheSizeLimitEndsTheConnection() throws Exception {
        startServer(headers -> Future.succeededFuture(Collections.emptyMap()), new StompServerOptions());
        client = StompClient.create(vertx, new StompClientOptions().setMaxFrameSize(1024));
        StompClientConnection connection = connect(Collections.emptyMap());
        CompletableFuture<Frame> received = new CompletableFuture<>();
        CompletableFuture<Throwable> closeCause = new CompletableFuture<>();
        connection.closeHandler(closeCause::complete);
        connection.subscribe("sub-1", "/queue/big", received::complete).await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(handler.subscribeFrames().poll(TIMEOUT_SECONDS, TimeUnit.SECONDS));

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
    }

    private void startServer(java.util.function.Function<Map<String, String>, Future<Map<String, String>>> connectAnswer,
                             StompServerOptions stompOptions) throws Exception {
        handler = new RecordingStompServerHandler(connectAnswer);
        HttpServerOptions httpOptions = new HttpServerOptions().setPort(port)
                                                               .setWebSocketSubProtocols(List.of(StompClient.SUB_PROTOCOL));
        deploymentId = vertx.deployVerticle(StompServerVerticleFactory.create(httpOptions,
                                                                              stompOptions.setWebsocketPath(WEBSOCKET_PATH),
                                                                              () -> handler,
                                                                              null))
                            .await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private StompClientConnection connect(Map<String, String> connectHeaders) throws Exception {
        WebSocketConnectOptions options = new WebSocketConnectOptions().setHost("localhost").setPort(port).setURI(WEBSOCKET_PATH);
        return client.connect(options, connectHeaders).toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static void assertArrayEquals(byte[] expected, byte[] actual) {
        org.junit.Assert.assertArrayEquals(expected, actual);
    }

}
