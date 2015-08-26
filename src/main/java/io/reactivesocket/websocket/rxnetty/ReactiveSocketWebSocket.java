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

import java.io.IOException;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.reactivesocket.Completable;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivex.netty.protocol.http.ws.WebSocketConnection;
import rx.Observable;
import rx.Single;

public class ReactiveSocketWebSocket {

	private final ReactiveSocket reactiveSocket;

	private ReactiveSocketWebSocket(ReactiveSocket reactiveSocket) {
		this.reactiveSocket = reactiveSocket;
		reactiveSocket.start();
	}

	/**
	 * Create a ReactiveSocketWebSocketClient from a client-side {@link WebSocketConnection}.
	 * <p>
	 * A client-side connection is one that initiated the connection with a server and will define the ReactiveSocket behaviors via the {@link ConnectionSetupPayload} that define mime-types, leasing
	 * behavior and other connection-level details.
	 * 
	 * @param connection
	 *            DuplexConnection of client-side initiated connection for the ReactiveSocket protocol to use.
	 * @param setup
	 *            ConnectionSetupPayload that defines mime-types and other connection behavior details.
	 * @param handler
	 *            (Optional) RequestHandler for responding to requests from the server. If 'null' requests will be responded to with "Not Found" errors.
	 * @param errorStream
	 *            (Optional) Callback for errors while processing streams over connection. If 'null' then error messages will be output to System.err.
	 * @return ReactiveSocket for start, shutdown and sending requests.
	 */
	public static ReactiveSocketWebSocket fromClientConnection(WebSocketConnection connection, ConnectionSetupPayload setup, RequestHandler handler, Consumer<Throwable> errorStream) {
		return new ReactiveSocketWebSocket(ReactiveSocket.fromClientConnection(
				new DuplexConnection() {
					@Override
					public Publisher<Frame> getInput() {
						return toPublisher(connection.getInput().map(frame -> {
							return Frame.from(frame.content().nioBuffer());
						}));
					}

					@Override
					public void addOutput(Publisher<Frame> o, Completable callback) {
						// had to use writeAndFlushOnEach instead of write for frames to get through
						// TODO determine if that's expected or not
						connection.writeAndFlushOnEach(toObservable(o)
								.map(frame -> new BinaryWebSocketFrame(Unpooled.wrappedBuffer(frame.getByteBuffer()))))
								.subscribe(n -> {
						} , callback::error, callback::success);
					}

					@Override
					public void close() throws IOException {
						connection.closeNow();
					}
				}, setup, handler, errorStream));
	}

	/**
	 * Create a ReactiveSocketWebSocketClient from a client-side {@link WebSocketConnection}.
	 * <p>
	 * A client-side connection is one that initiated the connection with a server and will define the ReactiveSocket behaviors via the {@link ConnectionSetupPayload} that define mime-types, leasing
	 * behavior and other connection-level details.
	 * <p>
	 * If this ReactiveSocket receives requests from the server it will respond with "Not Found" errors.
	 * 
	 * @param connection
	 *            DuplexConnection of client-side initiated connection for the ReactiveSocket protocol to use.
	 * @param setup
	 *            ConnectionSetupPayload that defines mime-types and other connection behavior details.
	 * @param errorStream
	 *            (Optional) Callback for errors while processing streams over connection. If 'null' then error messages will be output to System.err.
	 * @return ReactiveSocket for start, shutdown and sending requests.
	 */
	public static ReactiveSocketWebSocket fromClientConnection(WebSocketConnection connection, ConnectionSetupPayload setup, Consumer<Throwable> errorStream) {
		return fromClientConnection(connection, setup, null, errorStream);
	}

	/**
	 * Create a ReactiveSocketWebSocketClient from a client-side {@link WebSocketConnection}.
	 * <p>
	 * A client-side connection is one that initiated the connection with a server and will define the ReactiveSocket behaviors via the {@link ConnectionSetupPayload} that define mime-types, leasing
	 * behavior and other connection-level details.
	 * <p>
	 * If this ReactiveSocket receives requests from the server it will respond with "Not Found" errors.
	 * 
	 * @param connection
	 *            DuplexConnection of client-side initiated connection for the ReactiveSocket protocol to use.
	 * @param metadataMimeType
	 *            String mime-type for Metadata (this is sent via ConnectionSetupPayload on the initial Setup Frame).
	 * @param dataMimeType
	 *            String mime-type for Data (this is sent via ConnectionSetupPayload on the initial Setup Frame).
	 * @param errorStream
	 *            (Optional) Callback for errors while processing streams over connection. If 'null' then error messages will be output to System.err.
	 * @return ReactiveSocket for start, shutdown and sending requests.
	 */
	public static ReactiveSocketWebSocket fromClientConnection(WebSocketConnection connection, String metadataMimeType, String dataMimeType, Consumer<Throwable> errorStream) {
		return fromClientConnection(connection, ConnectionSetupPayload.create(metadataMimeType, dataMimeType), errorStream);
	}

	/**
	 * Create a ReactiveSocketWebSocketClient from a client-side {@link WebSocketConnection}.
	 * <p>
	 * A client-side connection is one that initiated the connection with a server and will define the ReactiveSocket behaviors via the {@link ConnectionSetupPayload} that define mime-types, leasing
	 * behavior and other connection-level details.
	 * <p>
	 * If this ReactiveSocket receives requests from the server it will respond with "Not Found" errors.
	 * 
	 * @param connection
	 *            DuplexConnection of client-side initiated connection for the ReactiveSocket protocol to use.
	 * @param mimeType
	 *            String mime-type for Data and Metadata (this is sent via ConnectionSetupPayload on the initial Setup Frame).
	 * @param errorStream
	 *            (Optional) Callback for errors while processing streams over connection. If 'null' then error messages will be output to System.err.
	 * @return ReactiveSocket for start, shutdown and sending requests.
	 */
	public static ReactiveSocketWebSocket fromClientConnection(WebSocketConnection connection, String mimeType, Consumer<Throwable> errorStream) {
		return fromClientConnection(connection, ConnectionSetupPayload.create(mimeType, mimeType), errorStream);
	}

	/**
	 * Create a ReactiveSocketWebSocketClient from a client-side {@link WebSocketConnection}.
	 * <p>
	 * A client-side connection is one that initiated the connection with a server and will define the ReactiveSocket behaviors via the {@link ConnectionSetupPayload} that define mime-types, leasing
	 * behavior and other connection-level details.
	 * <p>
	 * If this ReactiveSocket receives requests from the server it will respond with "Not Found" errors.
	 * <p>
	 * Error messages will be output to System.err.
	 * 
	 * @param connection
	 *            DuplexConnection of client-side initiated connection for the ReactiveSocket protocol to use.
	 * @param mimeType
	 *            String mime-type for Data and Metadata (this is sent via ConnectionSetupPayload on the initial Setup Frame).
	 * @return ReactiveSocket for start, shutdown and sending requests.
	 */
	public static ReactiveSocketWebSocket fromClientConnection(WebSocketConnection connection, String mimeType) {
		return fromClientConnection(connection, ConnectionSetupPayload.create(mimeType, mimeType), null);
	}

	/**
	 * Create a ReactiveSocketWebSocketServer from a server-side {@link WebSocketConnection}.
	 * <p>
	 * A server-side connection is one that accepted the connection from a client and will define the ReactiveSocket behaviors via the {@link ConnectionSetupPayload} that define mime-types, leasing
	 * behavior and other connection-level details.
	 * 
	 * @param connection
	 *            DuplexConnection of server-side connection for the ReactiveSocket protocol to use.
	 * @param connectionHandler
	 *            ConnectionSetupHandler that produces a {@link RequestHandler} for a given {@link ConnectionSetupPayload}
	 * @param errorStream
	 *            (Optional) Callback for errors while processing streams over connection. If 'null' then error messages will be output to System.err.
	 * @return ReactiveSocket for start, shutdown and sending requests.
	 */
	public static ReactiveSocketWebSocket fromServerConnection(WebSocketConnection connection, ConnectionSetupHandler connectionHandler, Consumer<Throwable> errorStream) {
		return new ReactiveSocketWebSocket(ReactiveSocket.fromServerConnection(new DuplexConnection() {
			@Override
			public Publisher<Frame> getInput() {
				return toPublisher(connection.getInput().map(frame -> {
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
			public void addOutput(Publisher<Frame> o, Completable callback) {
				// had to use writeAndFlushOnEach instead of write for frames to reliably get through
				// TODO determine if that's expected or not
				connection.writeAndFlushOnEach(toObservable(o).map(frame -> {
					return new BinaryWebSocketFrame(Unpooled.wrappedBuffer(frame.getByteBuffer()));
				})).subscribe(n -> {
				} , callback::error, callback::success);
			}

			@Override
			public void close() throws IOException {
				connection.closeNow();
			}
		}, connectionHandler, errorStream));
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

	public Observable<Payload> requestChannel(Observable<Payload> payloads) {
		assertConnection();
		return toObservable(reactiveSocket.requestChannel(toPublisher(payloads)));
	}

	private void assertConnection() {
		// TODO anything to do here?
	}


}
