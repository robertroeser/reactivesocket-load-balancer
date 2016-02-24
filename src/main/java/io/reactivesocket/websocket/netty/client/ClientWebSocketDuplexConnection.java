package io.reactivesocket.websocket.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
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
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClientWebSocketDuplexConnection implements DuplexConnection {
    private Channel channel;

    private Bootstrap bootstrap;

    private final CopyOnWriteArrayList<Observer<Frame>> subjects;

    private ClientWebSocketDuplexConnection(Channel channel, Bootstrap bootstrap,  CopyOnWriteArrayList<Observer<Frame>> subjects) {
        this.subjects  = subjects;
        this.channel = channel;
        this.bootstrap = bootstrap;
    }

    public static Publisher<ClientWebSocketDuplexConnection> create(InetSocketAddress address, EventLoopGroup eventLoopGroup) {

        Publisher<ClientWebSocketDuplexConnection> publisher = new Publisher<ClientWebSocketDuplexConnection>() {
            @Override
            public void subscribe(Subscriber<? super ClientWebSocketDuplexConnection> s) {
                WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory
                    .newHandshaker(
                        URI.create("ws://" + address.getHostName() + ":" + address.getPort() + "/rs"),
                        WebSocketVersion.V13,
                        null,
                        false,
                        new DefaultHttpHeaders());

                CopyOnWriteArrayList<Observer<Frame>> subjects = new CopyOnWriteArrayList<>();
                ReactiveSocketClientHandler clientHandler = new ReactiveSocketClientHandler(subjects);
                Bootstrap bootstrap = new Bootstrap();
                ChannelFuture connect = bootstrap
                    .group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(
                                new HttpClientCodec(),
                                new HttpObjectAggregator(8192),
                                new WebSocketClientProtocolHandler(handshaker),
                                clientHandler
                            );
                        }
                    })
                    .connect(address);

                connect.addListener(new GenericFutureListener<Future<? super Void>>() {
                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        if (future.cause() != null) {
                            s.onError(future.cause());
                        } else {
                            final Channel ch = connect.channel();
                            clientHandler
                                .getHandshakePromise()
                                .addListener(new GenericFutureListener<Future<? super Void>>() {
                                    @Override
                                    public void operationComplete(Future<? super Void> future) throws Exception {
                                        if (future.cause() != null) {
                                            s.onError(future.cause());
                                        } else {
                                            ClientWebSocketDuplexConnection duplexConnection = new ClientWebSocketDuplexConnection(ch, bootstrap, subjects);
                                            s.onNext(duplexConnection);
                                            s.onComplete();
                                        }
                                    }
                                });
                        }
                    }
                });
            }
        };

        return publisher;

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
                        ByteBuf byteBuf = Unpooled.wrappedBuffer(frame.getByteBuffer());
                        BinaryWebSocketFrame binaryWebSocketFrame = new BinaryWebSocketFrame(byteBuf);
                        ChannelFuture channelFuture = channel.writeAndFlush(binaryWebSocketFrame);
                        channelFuture.addListener((Future<? super Void> future) -> {
                            Throwable cause = future.cause();
                            if (cause != null) {
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
        channel.close();
    }

}
