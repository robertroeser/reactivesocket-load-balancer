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
package io.reactivesocket.rxnetty.websockets;

import static rx.Observable.*;
import static rx.RxReactiveStreams.*;

import org.reactivestreams.Publisher;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Message;
import io.reactivesocket.ReactiveSocketServerProtocol;
import io.reactivex.netty.protocol.http.ws.WebSocketConnection;
import rx.Observable;
import rx.Single;
import rx.functions.Func1;

/**
 * A ReactiveSocket handler for WebSockets in RxNetty.
 * <p>
 * Use this ReactiveSocketWebSockets with an RxNetty server similar to below.
 * 
 * <pre>
 *  {@code
 *	ReactiveSocketWebSockets handler = ReactiveSocketWebSockets.create(
 *			request -> {
 *				// handler logic for request/response (return a Single)
 *				return Single.just("hello" + request);
 *			},
 *			request -> {
 *				// handler logic for request/stream (return an Observable)
 *				return just("a_" + request, "b_" + request);
 *			});
 *
 *	// start server with protocol
 *	HttpServer.newServer().start((request, response) -> {
 *		return response.acceptWebSocketUpgrade(handler::acceptWebsocket);
 *	});
 * </pre>
 */
public class ReactiveSocketWebSockets {

	private final ReactiveSocketServerProtocol rsProtocol;

	Func1<String, Single<String>> requestResponseHandler;
	Func1<String, Observable<String>> requestStreamHandler;

	private ReactiveSocketWebSockets(
			Func1<String, Single<String>> requestResponseHandler,
			Func1<String, Observable<String>> requestStreamHandler) {
		this.requestResponseHandler = requestResponseHandler;
		this.requestStreamHandler = requestStreamHandler;
		// instantiate the ServerProtocol with handler converters from Observable to Publisher
		this.rsProtocol = ReactiveSocketServerProtocol.create(this::handleRequestResponse, this::handleRequestStream);
	}

	public static ReactiveSocketWebSockets create(
			Func1<String, Single<String>> requestResponseHandler,
			Func1<String, Observable<String>> requestStreamHandler) {
		return new ReactiveSocketWebSockets(requestResponseHandler, requestStreamHandler);
	}

	/**
	 * Use this method as the RxNetty HttpServer WebSocket handler.
	 * 
	 * @param ws
	 * @return
	 */
	public Observable<Void> acceptWebsocket(WebSocketConnection ws) {
		return toObservable(rsProtocol.acceptConnection(new DuplexConnection() {

			@Override
			public Publisher<Message> getInput() {
				return toPublisher(ws.getInput().map(frame -> {
					// TODO is this copying bytes?
					try {
						return Message.from(frame.content().nioBuffer());
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}));
			}

			@Override
			public Publisher<Void> write(Message o) {
				// BinaryWebSocketFrame frame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(o.getBytes()));
				TextWebSocketFrame frame = new TextWebSocketFrame(Unpooled.wrappedBuffer(o.getBytes()));
				return toPublisher(ws.write(just(frame)));
			}

		}));
	}

	private Publisher<String> handleRequestResponse(String request) {
		return toPublisher(requestResponseHandler.call(request).toObservable());
	}

	private Publisher<String> handleRequestStream(String request) {
		return toPublisher(requestStreamHandler.call(request));
	}
}
