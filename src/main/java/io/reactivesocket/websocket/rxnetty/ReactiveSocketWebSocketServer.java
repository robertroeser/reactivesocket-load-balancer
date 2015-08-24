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
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.reactivesocket.*;
import io.reactivex.netty.protocol.http.ws.WebSocketConnection;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.Single;
import rx.functions.Func1;
import rx.functions.Func2;

import static rx.RxReactiveStreams.toObservable;
import static rx.RxReactiveStreams.toPublisher;

/**
 * A ReactiveSocket handler for WebSockets in RxNetty.
 * <p>
 * Use this ReactiveSocketWebSockets with an RxNetty server similar to below.
 * 
 * <pre>
 * {@code
 *	ReactiveSocketWebSocketServer handler = ReactiveSocketWebSocketServer.create(handler);
 *
 * // start server with protocol
 * HttpServer.newServer().start((request, response) -> {
 * return response.acceptWebSocketUpgrade(handler::acceptWebsocket);
 * });
 * </pre>
 */
public class ReactiveSocketWebSocketServer {

    private final RequestHandler requestHandler;
	private final ReactiveSocket reactiveSocket;

    private ReactiveSocketWebSocketServer(ReactiveSocket reactiveSocket, RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
        this.reactiveSocket = reactiveSocket;
    }

    public static ReactiveSocketWebSocketServer create(
            Func1<Payload, Single<Payload>> requestResponseHandler,
            Func1<Payload, Observable<Payload>> requestStreamHandler,
            Func1<Payload, Observable<Payload>> requestSubscriptionHandler,
            Func1<Payload, Observable<Void>> fireAndForgetHandler,
            Func2<Payload, Observable<Payload>, Observable<Payload>> channelHandler) {
    	
    	RequestHandler handler = new RequestHandler() {

            @Override
            public Publisher<Payload> handleRequestResponse(Payload request) {
                return toPublisher(requestResponseHandler.call(request).toObservable());
            }

            @Override
            public Publisher<Payload> handleRequestStream(Payload request) {
                return toPublisher(requestStreamHandler.call(request));
            }

            @Override
            public Publisher<Payload> handleSubscription(Payload request) {
                return toPublisher(requestSubscriptionHandler.call(request));
            }

            @Override
            public Publisher<Void> handleFireAndForget(Payload request) {
                return toPublisher(fireAndForgetHandler.call(request));
            }

			@Override
			public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads) {
				return toPublisher(channelHandler.call(initialPayload, toObservable(payloads)));
			}

        };
        
        ReactiveSocket rs = ReactiveSocket.createResponderAndRequestor(handler);
    	
        return new ReactiveSocketWebSocketServer(rs, handler);
    }

    public static ReactiveSocketWebSocketServer create(RequestHandler handler) {
    	ReactiveSocket rs = ReactiveSocket.createResponderAndRequestor(handler);
        return new ReactiveSocketWebSocketServer(rs, handler);
    }

    /**
     * Use this method as the RxNetty HttpServer WebSocket handler.
     * 
     * @param ws
     * @return
     */
    public Observable<Void> acceptWebsocket(WebSocketConnection ws) {
    	return toObservable(reactiveSocket.connect(new DuplexConnection() {
			@Override
			public Publisher<Frame> getInput() {
				return toPublisher(ws.getInput().map(frame -> {
					// TODO is this copying bytes?
					try {
						return Frame.from(frame.content().nioBuffer());
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}));
			}

			@Override
			public Publisher<Void> addOutput(Publisher<Frame> o) {
				return toPublisher(ws.write(toObservable(o).map(frame -> {
					return new BinaryWebSocketFrame(Unpooled.wrappedBuffer(frame.getByteBuffer()));
				})));
			}
		}));
    }

}
