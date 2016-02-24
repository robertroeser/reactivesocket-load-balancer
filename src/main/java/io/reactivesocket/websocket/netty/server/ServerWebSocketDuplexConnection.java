package io.reactivesocket.websocket.netty.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
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

/**
 * Created by rroeser on 2/19/16.
 */
public class ServerWebSocketDuplexConnection implements DuplexConnection {
    private final CopyOnWriteArrayList<Observer<Frame>> subjects;

    private final ChannelHandlerContext ctx;

    public ServerWebSocketDuplexConnection(ChannelHandlerContext ctx) {
        this.subjects = new CopyOnWriteArrayList<>();
        this.ctx = ctx;
    }

    public List<? extends Observer<Frame>> getSubscribers() {
        return subjects;
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

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o
            .subscribe(new Subscriber<Frame>() {

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Frame frame) {
                    try {
                        ByteBuffer data = frame.getByteBuffer();
                        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);
                        BinaryWebSocketFrame binaryWebSocketFrame = new BinaryWebSocketFrame(byteBuf);
                        ChannelFuture channelFuture = ctx.writeAndFlush(binaryWebSocketFrame);
                        channelFuture.addListener((Future<? super Void> future) -> {
                            Throwable cause = future.cause();
                            if (cause != null) {
                                cause.printStackTrace();
                                callback.error(cause);
                            }
                        });
                    } catch (Throwable t) {
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

    }
}
