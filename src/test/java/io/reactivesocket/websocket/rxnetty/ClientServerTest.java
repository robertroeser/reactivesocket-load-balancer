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

import io.netty.buffer.ByteBuf;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.exceptions.SetupException;
import io.reactivesocket.websocket.rxnetty.server.ReactiveSocketWebSocketServer;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.ws.WebSocketConnection;
import io.reactivex.netty.protocol.http.ws.client.WebSocketResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.Observer;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClientServerTest {

    static ReactiveSocket client;
    static HttpServer<ByteBuf, ByteBuf> server;

    @BeforeClass
    public static void setup() {
        ReactiveSocketWebSocketServer serverHandler =
            ReactiveSocketWebSocketServer.create(new ConnectionSetupHandler() {
                @Override
                public RequestHandler apply(ConnectionSetupPayload setupPayload) throws SetupException {
                    return new RequestHandler() {
                        @Override
                        public Publisher<Payload> handleRequestResponse(Payload payload) {
                            return new Publisher<Payload>() {
                                @Override
                                public void subscribe(Subscriber<? super Payload> s) {
                                    //System.out.println("Handling request/response payload => " + s.toString());
                                    Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");
                                    s.onNext(response);
                                    s.onComplete();
                                }
                            };
                        }

                        @Override
                        public Publisher<Payload> handleRequestStream(Payload payload) {
                            Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");

                            return RxReactiveStreams
                                .toPublisher(Observable
                                    .range(1, 10)
                                    .map(i -> response));
                        }

                        @Override
                        public Publisher<Payload> handleSubscription(Payload payload) {
                            Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");

                            return RxReactiveStreams
                                .toPublisher(Observable
                                    .range(1, 10)
                                    .map(i -> response));
                        }

                        @Override
                        public Publisher<Void> handleFireAndForget(Payload payload) {
                            return Subscriber::onComplete;
                        }

                        @Override
                        public Publisher<Payload> handleChannel(Publisher<Payload> payloads) {
                            return null;
                        }

                        @Override
                        public Publisher<Void> handleMetadataPush(Payload payload) {
                            return null;
                        }
                    };
                }
            });

        server = HttpServer.newServer()
//			  .clientChannelOption(ChannelOption.AUTO_READ, true)
//            .enableWireLogging(LogLevel.ERROR)
            .start((req, resp) -> {
                return resp.acceptWebSocketUpgrade(serverHandler::acceptWebsocket);
            });


        Observable<WebSocketConnection> wsConnection = HttpClient.newClient("localhost", server.getServerPort())
            //.enableWireLogging(LogLevel.ERROR)
            .createGet("/rs")
            .requestWebSocketUpgrade()
            .flatMap(WebSocketResponse::getWebSocketConnection);

        Publisher<WebSocketDuplexConnection> connectionPublisher = WebSocketDuplexConnection.create(RxReactiveStreams.toPublisher(wsConnection));

        client = RxReactiveStreams
            .toObservable(connectionPublisher)
            .map(w -> ReactiveSocket.fromClientConnection(w, ConnectionSetupPayload.create
                ("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS)))
            .toBlocking()
            .single();

        client.startAndWait();
    }

    @AfterClass
    public static void tearDown() {
        server.shutdown();
    }

    @Test
    public void testRequestResponse1() {
        requestResponseN(1500, 1);
    }

    @Test
    public void testRequestResponse10() {
        requestResponseN(1500, 10);
    }


    @Test
    public void testRequestResponse100() {
        requestResponseN(1500, 100);
    }

    @Test
    public void testRequestResponse10_000() {
        requestResponseN(60_000, 10_000);
    }

    @Test
    public void testRequestStream() {
        TestSubscriber ts = TestSubscriber.create();

        RxReactiveStreams
            .toObservable(client.requestStream(TestUtil.utf8EncodedPayload("hello", "metadata")))
            .subscribe(ts);


        ts.awaitTerminalEvent(3_000, TimeUnit.MILLISECONDS);
        ts.assertValueCount(10);
        ts.assertNoErrors();
        ts.assertCompleted();
    }

    @Test
    public void testRequestSubscription() throws InterruptedException {
        TestSubscriber ts = TestSubscriber.create();

        CountDownLatch latch = new CountDownLatch(10);
        RxReactiveStreams
            .toObservable(client.requestSubscription(
                TestUtil.utf8EncodedPayload("hello sub", "metadata sub"))
            )
            .doOnEach(new Observer<Payload>() {
                @Override
                public void onCompleted() {}

                @Override
                public void onError(Throwable e) {}

                @Override
                public void onNext(Payload payload) {
                    latch.countDown();
                }
            })
            .subscribe(ts);
        latch.await();
        ts.assertValueCount(10);
        ts.assertNoErrors();
    }


    public void requestResponseN(int timeout, int count) {

        TestSubscriber ts = TestSubscriber.create();

        Observable
            .range(1, count)
            .flatMap(i ->
                    RxReactiveStreams
                        .toObservable(
                            client.requestResponse(TestUtil.utf8EncodedPayload("hello", "metadata"))
                        )
                        .map(payload ->
                                TestUtil.byteToString(payload.getData())
                        )
                        //.doOnNext(System.out::println)
            )
            .subscribe(ts);

        ts.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        ts.assertValueCount(count);
        ts.assertNoErrors();
        ts.assertCompleted();
    }


}