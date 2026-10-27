/*
 *
 * Copyright 2008-2021 Kinotic and the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vertx.ext.stomp.lite;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.VerticleBase;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.stomp.lite.frame.Frame;
import io.vertx.ext.stomp.lite.handler.StompServerWebSocketHandler;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Navid Mitchell on 2019-01-09.
 */
public class StompServerVerticle extends VerticleBase {

    private static final Logger log = LoggerFactory.getLogger(StompServerVerticle.class);

    private final HttpServerOptions httpOptions;
    private final StompServerOptions stompOptions;
    private final StompServerHandlerFactory stompServerHandlerFactory;
    private final Router router;
    private HttpServer httpServer;

    /**
     * Creates a StompServerVerticle
     * @param httpOptions to use with the underlying HttpServer
     * @param stompOptions to use with the StompServer
     * @param stompServerHandlerFactory to create StompServerHandlers
     * @param router to use for handling Http requests (can be null)
     */
    public StompServerVerticle(HttpServerOptions httpOptions,
                               StompServerOptions stompOptions,
                               StompServerHandlerFactory stompServerHandlerFactory,
                               Router router) {
        this.httpOptions = httpOptions;
        this.stompOptions = stompOptions;
        this.stompServerHandlerFactory = stompServerHandlerFactory;
        this.router = router;
    }

    @Override
    public Future<?> start() {
        StompServerWebSocketHandler ssWebSocketHandler =
                new StompServerWebSocketHandler(vertx, stompOptions, stompServerHandlerFactory);

        Router effectiveRouter = router != null ? router : Router.router(vertx);

        effectiveRouter.route(stompOptions.getWebsocketPath())
                       .handler(context -> {
                           if (context.request().canUpgradeToWebSocket()) {
                               ssWebSocketHandler.onRoutingContext(context);
                           } else {
                               context.response()
                                      .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
                                      .end();
                           }
                       });

        // the client requests the STOMP sub-protocol, and a handshake that does not echo it fails on the client
        HttpServerOptions effectiveHttpOptions = new HttpServerOptions(httpOptions);
        List<String> subProtocols = httpOptions.getWebSocketSubProtocols();
        if (subProtocols == null || !subProtocols.contains(Frame.WEBSOCKET_SUB_PROTOCOL)) {
            List<String> withStomp = new ArrayList<>(subProtocols != null ? subProtocols : List.of());
            withStomp.add(Frame.WEBSOCKET_SUB_PROTOCOL);
            effectiveHttpOptions.setWebSocketSubProtocols(withStomp);
        }

        httpServer = vertx.createHttpServer(effectiveHttpOptions)
                          .requestHandler(effectiveRouter)
                          .exceptionHandler(event -> log.error(
                                  "Stomp server Exception before completing Client Connection",
                                  event));

        return httpServer.listen();
    }

    @Override
    public Future<?> stop() {
        return httpServer.close();
    }
}

