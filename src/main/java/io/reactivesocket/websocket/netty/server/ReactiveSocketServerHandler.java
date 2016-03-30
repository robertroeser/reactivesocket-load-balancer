package io.reactivesocket.websocket.netty.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.Frame;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.websocket.netty.MutableDirectByteBuf;

import java.util.concurrent.ConcurrentHashMap;

@ChannelHandler.Sharable
public class ReactiveSocketServerHandler extends SimpleChannelInboundHandler<BinaryWebSocketFrame> {
    private ConcurrentHashMap<ChannelId, ServerWebSocketDuplexConnection> duplexConnections = new ConcurrentHashMap<>();

    private ConnectionSetupHandler setupHandler;

    private LeaseGovernor leaseGovernor;

    private ReactiveSocketServerHandler(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor) {
        this.setupHandler = setupHandler;
        this.leaseGovernor = leaseGovernor;
    }

    public static ReactiveSocketServerHandler create(ConnectionSetupHandler setupHandler) {
        return create(setupHandler, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR);
    }

    public static ReactiveSocketServerHandler create(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor) {
        return new
            ReactiveSocketServerHandler(
            setupHandler,
            leaseGovernor);

    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BinaryWebSocketFrame msg) throws Exception {
        ByteBuf content = msg.content();
        MutableDirectByteBuf mutableDirectByteBuf = new MutableDirectByteBuf(content);
        Frame from = Frame.from(mutableDirectByteBuf, 0, mutableDirectByteBuf.capacity());
        channelRegistered(ctx);
        ServerWebSocketDuplexConnection connection = duplexConnections.computeIfAbsent(ctx.channel().id(), i -> {
            System.out.println("No connection found for channel id: " + i);
            ServerWebSocketDuplexConnection c = new ServerWebSocketDuplexConnection(ctx);
            ReactiveSocket reactiveSocket = ReactiveSocket.fromServerConnection(c, setupHandler, leaseGovernor, throwable -> throwable.printStackTrace());
            reactiveSocket.startAndWait();
            return c;
        });
        if (connection != null) {
            connection
                .getSubscribers()
                .forEach(o -> o.onNext(from));
        }
    }
}
