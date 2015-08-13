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

import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelOption;
import io.netty.handler.logging.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.ws.client.WebSocketResponse;
import rx.Observable;
import rx.Single;
import rx.observers.TestSubscriber;

public class ClientServerTest {

    static Observable<ReactiveSocketWebSocketClient> client;
    static HttpServer<ByteBuf, ByteBuf> server;

    @BeforeClass
    public static void setup() {
        ReactiveSocketWebSocketServer serverHandler = ReactiveSocketWebSocketServer.create(
                requestResponse -> {
                    return Single.just(requestResponse + " world");
                } ,
                requestStream -> {
                    return Observable.just(requestStream, "world");
                } , null, null);

        server = HttpServer.newServer().clientChannelOption(ChannelOption.AUTO_READ, true).enableWireLogging(LogLevel.ERROR).start((req, resp) -> {
            return resp.acceptWebSocketUpgrade(serverHandler::acceptWebsocket);
        });

        client = HttpClient.newClient("localhost", server.getServerPort()).enableWireLogging(LogLevel.ERROR)
                .createGet("/rs")
                .requestWebSocketUpgrade()
                .flatMap(WebSocketResponse::getWebSocketConnection)
                .map(ReactiveSocketWebSocketClient::create);
    }

    @AfterClass
    public static void tearDown() {
        server.shutdown();
    }

    @Test
    public void testRequestResponse() {
        TestSubscriber<String> ts = TestSubscriber.create();
        client.flatMap(reactiveSocket -> {
            return reactiveSocket.requestResponse("hello").toObservable();
        }).subscribe(ts);
        ts.awaitTerminalEvent(5000000, TimeUnit.MILLISECONDS);
        ts.assertValue("hello world");
    }

    @Test
    public void testRequestResponseMultiple() {
        TestSubscriber<String> ts = TestSubscriber.create();
        client.flatMap(reactiveSocket -> {
            return reactiveSocket.requestResponse("hello")
                    .mergeWith(reactiveSocket.requestResponse("hi"));
        }).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertValues("hello world", "hi world");
    }

    @Test
    public void testRequestStream() {
        TestSubscriber<String> ts = TestSubscriber.create();
        client.flatMap(reactiveSocket -> {
            return reactiveSocket.requestStream("hello");
        }).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertValues("hello", "world");
    }

}
