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

import org.junit.Test;
import org.reactivestreams.Publisher;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.ws.client.WebSocketResponse;
import rx.Observable;

public class WebSocketTest {

	@Test
	public void testPingPong() {
		HttpServer<ByteBuf, ByteBuf> server = HttpServer.newServer();
		server.start((request, response) -> {
			return response.acceptWebSocketUpgrade(ws -> {
				DuplexConnection serverConnection = new DuplexConnection() {
					@Override
					public Publisher<Frame> getInput() {
						return toPublisher(ws.getInput().map(frame -> {
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
						return toPublisher(ws.writeAndFlushOnEach(toObservable(o).map(frame -> {
							System.out.println("*** writing response from server: " + frame);
							return new BinaryWebSocketFrame(Unpooled.wrappedBuffer(frame.getByteBuffer()));
						})));
					}
				};

				return toObservable(serverConnection
						.getInput()).flatMap(frame -> {
					System.out.println("Received frame on server: " + frame);
					return toObservable(serverConnection.addOutput(toPublisher(
							Observable.just(Frame.from(1, FrameType.NEXT_COMPLETE,
									TestUtil.utf8EncodedPayloadData("response data"))))));
				});
			});
		});

		HttpClient<ByteBuf, ByteBuf> client = HttpClient.newClient(server.getServerAddress());

		client.createGet("/")
				.requestWebSocketUpgrade()
				.flatMap(WebSocketResponse::getWebSocketConnection)
				.flatMap(ws -> {
					DuplexConnection clientConnection = new DuplexConnection() {
						@Override
						public Publisher<Frame> getInput() {
							return toPublisher(ws.getInput()
									.doOnSubscribe(() -> System.out.println("subscribing to input on client"))
									.doOnUnsubscribe(() -> System.out.println("unsubscribe to input on client"))
									.map(frame -> {
								System.out.println("response: " + Frame.from(frame.content().nioBuffer()));
								return Frame.from(frame.content().nioBuffer());
							}));
						}

						@Override
						public Publisher<Void> addOutput(Publisher<Frame> o) {
							Publisher<Void> p = toPublisher(ws.writeAndFlushOnEach(toObservable(o)
									.doOnNext(frame -> System.out.println("*** writing request from client: " + frame))
									.map(frame -> new BinaryWebSocketFrame(Unpooled.wrappedBuffer(frame.getByteBuffer())))));
							return p;
						}
					};

					return toObservable(
							clientConnection.addOutput(toPublisher(Observable.just(Frame.from(1, FrameType.REQUEST_RESPONSE,
									TestUtil.utf8EncodedPayloadData("request data")))))).cast(Frame.class)
											.mergeWith(toObservable(clientConnection.getInput()));
				}).take(1).toBlocking().forEach(d -> System.out.println("Received on client: " + d));
		
		server.shutdown();

	}

}
