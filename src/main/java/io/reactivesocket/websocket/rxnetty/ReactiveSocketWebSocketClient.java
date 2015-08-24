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

import static rx.RxReactiveStreams.*;

import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Publisher;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivex.netty.protocol.http.ws.WebSocketConnection;
import rx.Observable;
import rx.Single;

public class ReactiveSocketWebSocketClient {

	private final ReactiveSocket reactiveSocket;
	private Publisher<Void> connect;
	private final AtomicBoolean connected = new AtomicBoolean(false);
	private final AtomicBoolean terminated = new AtomicBoolean(false);

	private ReactiveSocketWebSocketClient(WebSocketConnection wsConn) {
		this.reactiveSocket = ReactiveSocket.createRequestor();
		connect = this.reactiveSocket.connect(
				new DuplexConnection() {
					@Override
					public Publisher<Frame> getInput() {
						return toPublisher(wsConn.getInput().map(frame -> {
							return Frame.from(frame.content().nioBuffer());
						}));
					}

					@Override
					public Publisher<Void> addOutput(Publisher<Frame> o) {
						// had to use writeAndFlushOnEach instead of write for frames to get through
						// TODO determine if that's expected or not
						Publisher<Void> p = toPublisher(wsConn.writeAndFlushOnEach(toObservable(o)
								.map(frame -> new BinaryWebSocketFrame(Unpooled.wrappedBuffer(frame.getByteBuffer())))
						).doOnSubscribe(() -> System.out.println("DuplexConnection subscribe to Netty writeAndFlush " + System.currentTimeMillis()))
								);
						return p;
					}
				});
	}

	public static ReactiveSocketWebSocketClient create(WebSocketConnection ws) {
		return new ReactiveSocketWebSocketClient(ws);
	}

	public Single<Payload> requestResponse(Payload request) {
		assertConnection();
		return toObservable(reactiveSocket.requestResponse(request)).toSingle();
	}

	public Observable<Payload> requestStream(Payload request) {
		assertConnection();
		return toObservable(reactiveSocket.requestStream(request));
	}

	public Observable<Payload> requestSubscription(Payload topic) {
		assertConnection();
		return toObservable(reactiveSocket.requestSubscription(topic));
	}

	public Single<Void> fireAndForget(Payload request) {
		assertConnection();
		return toObservable(reactiveSocket.fireAndForget(request)).toSingle();
	}

	private void assertConnection() {
		if (!connected.get()) {
			throw new IllegalStateException("connect().subscribe() must occur first.");
		}
		if (terminated.get()) {
			throw new IllegalStateException("connection is terminated");
		}
	}

	/**
	 * Connect the underlying channel to permit requests.
	 * 
	 * Start, monitor errors, and disconnect with the returned Single.
	 * 
	 * @return
	 */
	public Single<Void> connect() {
		// TODO what should we do if somehow double subscribes?
		return toObservable(connect)
				.doOnSubscribe(() -> connected.set(true))
				.doOnTerminate(() -> terminated.set(true)).toSingle();
	}

}
