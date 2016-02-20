package io.reactivesocket.websocket.rxnetty.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.CharsetUtil;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Observer;
import io.reactivesocket.websocket.rxnetty.MutableDirectByteBuf;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {

    private final ConcurrentHashMap<ChannelId, ClientWebSocketDuplexConnection> duplexConnections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChannelId, WebSocketClientHandshaker> handshakers = new ConcurrentHashMap<>();

    public WebSocketClientHandler() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        WebSocketClientHandshaker handshaker = handshakers.get(ctx.channel().id());
        handshaker.handshake(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        System.out.println("WebSocket Client disconnected!");
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        WebSocketClientHandshaker handshaker = handshakers.computeIfAbsent(ctx.channel().id(), i -> null);
        Channel ch = ctx.channel();
        if (!handshaker.isHandshakeComplete()) {
            handshaker.finishHandshake(ch, (FullHttpResponse) msg);
            System.out.println("WebSocket Client connected!");
            return;
        }

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new IllegalStateException(
                "Unexpected FullHttpResponse (getStatus=" + response.status() +
                    ", content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
        }

        WebSocketFrame webSocketFrame = (WebSocketFrame) msg;

        // Check for closing frame
        if (webSocketFrame instanceof CloseWebSocketFrame) {
            handshaker.close(ctx.channel(), (CloseWebSocketFrame) webSocketFrame.retain());
            ch.close();
            return;
        }

        if (!(webSocketFrame instanceof BinaryWebSocketFrame)) {
            throw new UnsupportedOperationException(String.format("%s frame types not supported", webSocketFrame.getClass()
                .getName()));
        }

        ClientWebSocketDuplexConnection duplexConnection = duplexConnections.get(ctx.channel().id());

        BinaryWebSocketFrame binaryWebSocketFrame = (BinaryWebSocketFrame) webSocketFrame;
        ByteBuf content = binaryWebSocketFrame.content();
        MutableDirectByteBuf mutableDirectByteBuf = new MutableDirectByteBuf(content);
        Frame frame = Frame.from(mutableDirectByteBuf, 0, mutableDirectByteBuf.capacity());
        List<? extends Observer<Frame>> subscribers = duplexConnection.getSubscribers();
        subscribers
            .forEach(o -> o.onNext(frame));

    }

    void addDuplexConnection(ChannelId channelId, ClientWebSocketDuplexConnection duplexConnection, WebSocketClientHandshaker handshaker) {
        handshakers.put(channelId, handshaker);
        duplexConnections.put(channelId, duplexConnection);
    }

    void removeDuplexConnection(ChannelId channelId) {
        duplexConnections.remove(channelId);
        handshakers.remove(channelId);
    }
}