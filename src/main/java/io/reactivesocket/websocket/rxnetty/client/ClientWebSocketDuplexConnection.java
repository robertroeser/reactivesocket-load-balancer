package io.reactivesocket.websocket.rxnetty.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.util.concurrent.Future;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClientWebSocketDuplexConnection implements DuplexConnection {

    private final CopyOnWriteArrayList<Observer<Frame>> subjects;

    private Channel channel;

    private WebSocketClientHandshaker webSocketClientHandshaker;

    public ClientWebSocketDuplexConnection(Channel channel, WebSocketClientHandshaker webSocketClientHandshaker) {
        this.channel = channel;
        subjects = new CopyOnWriteArrayList<>();
        ChannelPipeline channelPipeline = channel.pipeline();
        WebSocketClientHandler webSocketClientHandler = channelPipeline.get(WebSocketClientHandler.class);
        if (webSocketClientHandler == null) {
            synchronized (this) {
                webSocketClientHandler = channelPipeline.get(WebSocketClientHandler.class);
                if (webSocketClientHandler == null) {
                    webSocketClientHandler = new WebSocketClientHandler();
                    channelPipeline.addLast(webSocketClientHandler);
                }
            }
        }

        webSocketClientHandler.addDuplexConnection(channel.id(), this, webSocketClientHandshaker);
    }

    @Override
    public final Observable<Frame> getInput() {
        return new Observable<Frame>() {
            public void subscribe(Observer<Frame> o) {
                o.onSubscribe(new Disposable() {
                    @Override
                    public void dispose() {
                        subjects.removeIf(s -> s == o);
                    }
                });

                subjects.add(o);
            }
        };
    }

    public List<? extends Observer<Frame>> getSubscribers() {
        return subjects;
    }


    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o
            .subscribe(new Subscriber<Frame>() {
                private Subscription s;

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(128);
                    this.s = s;
                }

                @Override
                public void onNext(Frame frame) {
                    try {
                        ByteBuffer byteBuffer = frame.getByteBuffer();
                        ByteBuf buf = PooledByteBufAllocator
                            .DEFAULT
                            .buffer(byteBuffer.capacity());
                        buf.writeBytes(byteBuffer);

                        BinaryWebSocketFrame binaryWebSocketFrame = new BinaryWebSocketFrame(buf);

                        ChannelFuture channelFuture = channel.writeAndFlush(binaryWebSocketFrame);
                        channelFuture.addListener((Future<? super Void> future) -> {
                            try {
                                Throwable cause = future.cause();
                                if (cause != null) {
                                    callback.error(cause);
                                }
                            } finally {
                                s.request(1);
                            }
                        });
                    } catch (Throwable t) {
                        s.request(1);
                        onError(t);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    callback.error(t);
                }

                @Override
                public void onComplete() {
                    callback.success();
                }
            });
    }

    @Override
    public void close() throws IOException {
        WebSocketClientHandler webSocketClientHandler = channel.pipeline().get(WebSocketClientHandler.class);
        if (webSocketClientHandler != null) {
            webSocketClientHandler.removeDuplexConnection(channel.id());
        }
    }
}
