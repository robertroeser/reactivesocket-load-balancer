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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.RxReactiveStreams;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CopyOnWriteArrayList;

public class WebSocketDuplexConnection implements DuplexConnection {
    protected static ThreadLocal<MutableDirectByteBuf> mutableDirectByteBufs = ThreadLocal.withInitial(() -> new MutableDirectByteBuf(Unpooled.buffer()));

    protected WebSocketConnection webSocketConnection;

    protected CopyOnWriteArrayList<Observer<Frame>> observers;

    protected WebSocketDuplexConnection(WebSocketConnection webSocketConnection) {
        this.webSocketConnection = webSocketConnection;
        this.observers = new CopyOnWriteArrayList<>();

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
                    ByteBuf content = webSocketFrame.content();
                    try {
                        MutableDirectByteBuf buffer = mutableDirectByteBufs.get();
                        buffer.wrap(content);
                        Frame frame = Frame.from(buffer, 0, buffer.capacity());

                        observers
                            .forEach(o -> o.onNext(frame));
                    } finally {
                        if (content != null) {
                            content.release();
                        }
                    }
                }
            });

    }

    public static WebSocketDuplexConnection create(WebSocketConnection webSocketConnection) {
        return new WebSocketDuplexConnection(webSocketConnection);
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
                            child.onNext(create(webSocketConnection));
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

        rx.Observable<WebSocketFrame> binaryWebSocketFrameObservable = RxReactiveStreams
            .toObservable(o)
            .map(frame -> {
                ByteBuffer byteBuffer = frame.getByteBuffer();
                ByteBuf buf = PooledByteBufAllocator
                    .DEFAULT
                    .buffer(byteBuffer.capacity());
                buf.writeBytes(byteBuffer);

                return new BinaryWebSocketFrame(buf);
            });


        webSocketConnection
            .writeAndFlushOnEach(binaryWebSocketFrameObservable)
            .doOnCompleted(callback::success)
            .doOnError(callback::error)
                .subscribe();
            }

        @Override
    public void close() throws IOException {
        webSocketConnection.close(true);
    }
}
