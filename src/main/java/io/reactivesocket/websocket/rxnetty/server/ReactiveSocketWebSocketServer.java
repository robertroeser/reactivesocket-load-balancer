/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.websocket.rxnetty.server;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.LeaseGovernor;

public class ReactiveSocketWebSocketServer implements AutoCloseable {
    private InternalLogger logger = InternalLoggerFactory.getInstance(ReactiveSocketWebSocketServer.class);

    private final ConnectionSetupHandler setupHandler;

    private final LeaseGovernor leaseGovernor;

    private Channel channel;

    private SslContext sslContext;

    private ReactiveSocketWebSocketServer(
        ConnectionSetupHandler setupHandler,
        LeaseGovernor leaseGovernor,
        Channel channel,
        SslContext sslContext) {
        this.setupHandler = setupHandler;
        this.leaseGovernor = leaseGovernor;
        this.channel = channel;
        this.sslContext = sslContext;

        init();
    }

    public static ReactiveSocketWebSocketServer create(ConnectionSetupHandler setupHandler, Channel channel) {
        return create(setupHandler, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR, channel, null);
    }

    public static ReactiveSocketWebSocketServer create(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor, Channel channel, SslContext sslContext) {
        return new
            ReactiveSocketWebSocketServer(
            setupHandler,
            leaseGovernor,
            channel,
            sslContext);

    }

    void init() {
        channel.pipeline().addLast(new WebSocketServerInitializer(sslContext));
    }

    @Override
    public void close() throws Exception {
    }
}
