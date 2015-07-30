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
package io.reactivesocket.websocket.rxnetty;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.ReactiveSocketClientProtocol;
import io.reactivex.netty.protocol.http.ws.WebSocketConnection;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.Single;

import static rx.RxReactiveStreams.toObservable;
import static rx.RxReactiveStreams.toPublisher;

public class ReactiveSocketWebSocketClient {

    private final ReactiveSocketClientProtocol rsProtocol;

    private ReactiveSocketWebSocketClient(WebSocketConnection wsConn) {
        this.rsProtocol = ReactiveSocketClientProtocol
                .create(new DuplexConnection() {

                    @Override
                    public Publisher<Frame> getInput() {
                        return toPublisher(wsConn.getInput().map(frame -> {
                            return Frame.from(frame.content().nioBuffer());
                        }));
                    }

                    @Override
                    public Publisher<Void> write(Publisher<Frame> o) {
                        // had to use writeAndFlushOnEach instead of write for frames to get through
                        // TODO determine if that's expected or not
                        return toPublisher(wsConn.writeAndFlushOnEach(toObservable(o).map(m -> {
                            // return new BinaryWebSocketFrame(Unpooled.wrappedBuffer(m.getBytes()));
                            return new TextWebSocketFrame(Unpooled.wrappedBuffer(m.getByteBuffer()));
                        })));
                    }

                });
    }

    public static ReactiveSocketWebSocketClient create(WebSocketConnection ws) {
        return new ReactiveSocketWebSocketClient(ws);
    }

    public Single<String> requestResponse(String request) {
        return toObservable(rsProtocol.requestResponse(request)).toSingle();
    }

    public Observable<String> requestStream(String request) {
        return toObservable(rsProtocol.requestStream(request));
    }

    public Observable<String> requestSubscription(String topic) {
        return toObservable(rsProtocol.requestSubscription(topic));
    }

    public Single<Void> fireAndForget(String request) {
        return toObservable(rsProtocol.fireAndForget(request)).toSingle();
    }

}
