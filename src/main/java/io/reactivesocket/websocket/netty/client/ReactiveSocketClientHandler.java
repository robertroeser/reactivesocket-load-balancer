package io.reactivesocket.websocket.netty.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Observer;
import io.reactivesocket.websocket.netty.MutableDirectByteBuf;

import java.util.concurrent.CopyOnWriteArrayList;

@ChannelHandler.Sharable
public class ReactiveSocketClientHandler extends SimpleChannelInboundHandler<BinaryWebSocketFrame> {

    private final CopyOnWriteArrayList<Observer<Frame>> subjects;

    private ChannelPromise handshakePromise;

    public ReactiveSocketClientHandler(CopyOnWriteArrayList<Observer<Frame>> subjects) {
        this.subjects = subjects;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        this.handshakePromise = ctx.newPromise();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BinaryWebSocketFrame bFrame) throws Exception {
        ByteBuf content = bFrame.content();
        MutableDirectByteBuf mutableDirectByteBuf = new MutableDirectByteBuf(content);
        final Frame from = Frame.from(mutableDirectByteBuf, 0, mutableDirectByteBuf.capacity());
        subjects.forEach(o -> o.onNext(from));
    }

    public ChannelPromise getHandshakePromise() {
        return handshakePromise;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent) {
            WebSocketClientProtocolHandler.ClientHandshakeStateEvent evt1 = (WebSocketClientProtocolHandler.ClientHandshakeStateEvent) evt;
            if (evt1.equals(WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE)) {
                handshakePromise.setSuccess();
            }
        }
    }
}
