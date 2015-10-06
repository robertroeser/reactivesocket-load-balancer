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

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.websocket.rxnetty.WebSocketDuplexConnection;
import io.reactivex.netty.protocol.http.ws.WebSocketConnection;
import rx.Observable;
import uk.co.real_logic.agrona.LangUtil;

import java.util.concurrent.ConcurrentHashMap;

public class ReactiveSocketWebSocketServer {
    private final ConcurrentHashMap<WebSocketDuplexConnection, ReactiveSocket> reactiveSockets;

    private final ConnectionSetupHandler setupHandler;

    private final LeaseGovernor leaseGovernor;

    private ReactiveSocketWebSocketServer(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor) {
        this.reactiveSockets = new ConcurrentHashMap<>();
        this.setupHandler = setupHandler;
        this.leaseGovernor = leaseGovernor;
    }

    public static ReactiveSocketWebSocketServer create(ConnectionSetupHandler setupHandler) {
        return create(setupHandler, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR);
    }

    public static ReactiveSocketWebSocketServer create(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor) {
        return new ReactiveSocketWebSocketServer(setupHandler, leaseGovernor);
    }

    public Observable<Void> acceptWebsocket(WebSocketConnection wsConnection) {
        return Observable
            .create(subscriber -> {
                WebSocketDuplexConnection webSocketDuplexConnection = WebSocketDuplexConnection
                    .create(wsConnection);

                ReactiveSocket reactiveSocket = ReactiveSocket
                    .fromServerConnection(
                        webSocketDuplexConnection,
                        setupHandler,
                        leaseGovernor,
                        t -> t.printStackTrace());

                reactiveSocket.start(new Completable() {
                    @Override
                    public void success() {
                    }

                    @Override
                    public void error(Throwable e) {
                        subscriber.onError(e);
                    }
                });

                reactiveSockets
                    .putIfAbsent(webSocketDuplexConnection, reactiveSocket);

                wsConnection
                    .closeListener()
                    .doOnCompleted(() -> {
                        reactiveSockets
                            .remove(webSocketDuplexConnection);

                        try {
                            reactiveSocket.close();
                        } catch (Exception e) {
                            LangUtil.rethrowUnchecked(e);
                        }
                    });
            });
    }

}
