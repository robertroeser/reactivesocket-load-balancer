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

import io.reactivesocket.websocket.rxnetty.WebSocketDuplexConnection;
import io.reactivex.netty.protocol.http.ws.WebSocketConnection;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

class WebSocketServerDuplexConnection extends WebSocketDuplexConnection {
    public WebSocketServerDuplexConnection(WebSocketConnection webSocketConnection) {
        super(webSocketConnection, "server");
    }

    public static Publisher<WebSocketDuplexConnection> create(Publisher<WebSocketConnection> webSocketConnection) {
        Publisher<WebSocketDuplexConnection> duplexConnectionPublisher = new Publisher<WebSocketDuplexConnection>() {
            @Override
            public void subscribe(Subscriber<? super WebSocketDuplexConnection> child) {
                webSocketConnection
                    .subscribe(new Subscriber<WebSocketConnection>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            s.request(Long.MAX_VALUE);
                        }

                        @Override
                        public void onNext(WebSocketConnection webSocketConnection) {
                            WebSocketDuplexConnection connection = new WebSocketServerDuplexConnection(webSocketConnection);
                            child.onNext(connection);
                        }

                        @Override
                        public void onError(Throwable t) {
                            child.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            child.onComplete();
                        }
                    });
            }
        };

        return duplexConnectionPublisher;
    }
}
