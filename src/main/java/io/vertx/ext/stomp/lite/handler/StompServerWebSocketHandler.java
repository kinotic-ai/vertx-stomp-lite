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

package io.vertx.ext.stomp.lite.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.stomp.lite.StompServerHandler;
import io.vertx.ext.stomp.lite.StompServerHandlerFactory;
import io.vertx.ext.stomp.lite.StompServerOptions;
import io.vertx.ext.stomp.lite.frame.FrameParser;
import io.vertx.ext.stomp.lite.frame.InvalidConnectFrame;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Created by Navid Mitchell on 2019-02-04.
 */
public class StompServerWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(StompServerWebSocketHandler.class);

    private final Vertx vertx;
    private final StompServerOptions options;
    private final StompServerHandlerFactory factory;

    public StompServerWebSocketHandler(Vertx vertx,
                                       StompServerOptions options,
                                       StompServerHandlerFactory factory) {
        this.vertx = vertx;
        this.options = options;
        this.factory = factory;
    }

    public void onRoutingContext(RoutingContext routingContext) {
        HttpServerRequest request = routingContext.request();
        request.pause();
        StompServerHandler stompServerHandler = factory.create();
        stompServerHandler
                .handshake(routingContext)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        MultiMap responseHeaders = ar.result();
                        if (responseHeaders != null) {
                            request.response().headers().addAll(responseHeaders);
                        }
                        request.toWebSocket()
                                 .onSuccess(socket -> configureServerWebSocket(socket, stompServerHandler))
                                 .onFailure(throwable -> log.error("Could not upgrade request to WebSocket", throwable));
                    } else {
                        request.response().setStatusCode(HttpResponseStatus.UNAUTHORIZED.code()).end();
                    }
                });
    }

    private void configureServerWebSocket(ServerWebSocket socket, StompServerHandler stompServerHandler) {
        DefaultStompServerConnection defaultStompServerConnection = new DefaultStompServerConnection(socket,
                                                                                                     vertx,
                                                                                                     options,
                                                                                                     stompServerHandler);
        socket.exceptionHandler((exception) -> {
            boolean skip = exception instanceof VertxException && exception.getMessage().equals("Connection was closed");
            if (!skip) {
                log.debug("The STOMP server caught a WebSocket error - closing connection", exception);
                defaultStompServerConnection.clientCausedException(exception);
            }
        });

        socket.closeHandler( v -> defaultStompServerConnection.close());

        FrameParser parser = new FrameParser(options);
        parser.errorHandler(exception -> defaultStompServerConnection.clientCausedException(exception))
              .handler(defaultStompServerConnection);

        socket.handler(buffer -> {
            // Additional check to make sure that we don't parse a bunch of data when the client has not successfully connected
            if(!defaultStompServerConnection.isConnected()) {
                // client has not connected yet make ensure the client is sending a connect frame without parsing it completely
                if(buffer.length() > options.getMaxConnectFrameLength()){

                    log.debug("Client sent a frame larger than the maximum allowed connect frame");
                    // frame is incomplete
                    defaultStompServerConnection.clientCausedException(
                            new InvalidConnectFrame("Client sent a frame larger than the maximum allowed connect frame", buffer));

                }else if (buffer.length() > 7){

                    String possibleConnectCommand = new String(buffer.getBytes(0, 7));
                    if(possibleConnectCommand.equals("CONNECT")){
                        // initial frame looks like a connect frame try to parse
                        FrameParser connectParser = new FrameParser(options);
                        connectParser.errorHandler(exception -> defaultStompServerConnection.clientCausedException(new InvalidConnectFrame("Error parsing connect frame", exception, buffer)))
                                     .handler(defaultStompServerConnection);
                        connectParser.handle(buffer);
                    }else{
                        log.debug("Initial frame does not contain a connect command");
                        // frame is incorrect
                        defaultStompServerConnection.clientCausedException(
                                new InvalidConnectFrame("Initial frame does not contain a connect command", buffer));
                    }

                }else{
                    log.debug("Client sent an incomplete connect frame");
                    // frame is incomplete
                    defaultStompServerConnection.clientCausedException(
                            new InvalidConnectFrame("Client sent an incomplete connect frame", buffer));
                }
            }else{
                parser.handle(buffer);
            }
        });
    }
}
