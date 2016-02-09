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
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
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
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.util.concurrent.TimeUnit;

public class ClientServerTest {

    static ReactiveSocket client;
    static HttpServer<ByteBuf, ByteBuf> server;

    @BeforeClass
    public static void setup() {
        ReactiveSocketWebSocketServer serverHandler =
            ReactiveSocketWebSocketServer.create(setupPayload -> new RequestHandler() {
                @Override
                public Publisher<Payload> handleRequestResponse(Payload payload) {
                    return s -> {
                        //System.out.println("Handling request/response payload => " + s.toString());
                        Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");
                        s.onNext(response);
                        s.onComplete();
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
                            .map(i -> response)
                            .repeat());
                }

                @Override
                public Publisher<Void> handleFireAndForget(Payload payload) {
                    return Subscriber::onComplete;
                }

                @Override
                public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> inputs) {
                    return null;
                }

                @Override
                public Publisher<Void> handleMetadataPush(Payload payload) {
                    return null;
                }
            });

        server = HttpServer.newServer()
//			  .clientChannelOption(ChannelOption.AUTO_READ, true)
//            .enableWireLogging(LogLevel.ERROR)
            .start((req, resp) ->
                resp.acceptWebSocketUpgrade(serverHandler::acceptWebsocket)
            );

        Observable<WebSocketConnection> wsConnection = HttpClient.newClient(server.getServerAddress())
            //.enableWireLogging(LogLevel.ERROR)
            .createGet("/rs")
            .requestWebSocketUpgrade()
            .flatMap(WebSocketResponse::getWebSocketConnection);

        client = wsConnection.map(w ->
            ReactiveSocket.fromClientConnection(
                WebSocketDuplexConnection.create(w),
                ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS))
        ).toSingle().toBlocking().value();

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
        TestSubscriber<Payload> ts = TestSubscriber.create();

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
        TestSubscriber<Payload> ts = TestSubscriber.create();

        RxReactiveStreams
            .toObservable(client.requestSubscription(TestUtil.utf8EncodedPayload("hello sub", "metadata sub")))
            .take(10)
            .subscribe(ts);

        ts.awaitTerminalEvent(3_000, TimeUnit.MILLISECONDS);
        ts.assertValueCount(10);
        ts.assertNoErrors();
    }


    public void requestResponseN(int timeout, int count) {

        TestSubscriber<String> ts = TestSubscriber.create();

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