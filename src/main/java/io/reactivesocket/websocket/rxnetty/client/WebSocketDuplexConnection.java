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
package io.reactivesocket.websocket.rxnetty.client;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import io.reactivex.netty.protocol.http.ws.WebSocketConnection;
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class WebSocketDuplexConnection implements DuplexConnection {
    protected WebSocketConnection webSocketConnection;

    protected CopyOnWriteArrayList<Observer<Frame>> observers;

    private final String type;

    protected WebSocketDuplexConnection(WebSocketConnection webSocketConnection, String type) {
        this.webSocketConnection = webSocketConnection;
        this.observers = new CopyOnWriteArrayList<>();
        this.type = type;

        webSocketConnection
            .getInput()
            .unsafeSubscribe(new rx.Subscriber<WebSocketFrame>() {
                @Override
                public void onCompleted() {
                    observers
                        .forEach(Observer::onComplete);
                }

                @Override
                public void onError(Throwable e) {
                    observers
                        .forEach(o -> o.onError(e));
                }

                @Override
                public void onNext(WebSocketFrame webSocketFrame) {
                    // TODO use mutable direct buffer wrapper eventually
                    ByteBuffer buffer = webSocketFrame.content().nioBuffer();
                    Frame frame = Frame.from(buffer);

                    observers
                        .forEach(o -> o.onNext(frame));
                }
            });

    }

    @Override
    public Observable<Frame> getInput() {
        Observable<Frame> observable = new Observable<Frame>() {
            @Override
            public void subscribe(Observer<Frame> o) {
                observers.add(o);

                o.onSubscribe(new Disposable() {
                    @Override
                    public void dispose() {
                        observers.removeIf(s -> s == o);
                    }
                });
            }
        };

        return observable;
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        webSocketConnection
            .writeAndFlushOnEach(
                RxReactiveStreams
                    .toObservable(o)
                    .map(frame
                        -> new BinaryWebSocketFrame(Unpooled.wrappedBuffer(frame.getByteBuffer())))
            )
            .doOnCompleted(callback::success)
            .doOnError(t -> {
                try {
                    throw new RuntimeException(type, t);
                } catch (Throwable tt) {
                    tt.printStackTrace();
                }
                callback.error(t);
            })
                .subscribe();
            }

        @Override
    public void close() throws IOException {
        webSocketConnection.close(true);
    }
}
