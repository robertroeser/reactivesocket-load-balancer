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

import static rx.Observable.*;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.websocket.rxnetty.ReactiveSocketWebSocketServer;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.server.HttpServer;
import rx.Single;

public class ReactiveSocketWebSocketServerTest {

    @Test
    public void test() {
        // create protocol with handlers
        ReactiveSocketWebSocketServer handler = ReactiveSocketWebSocketServer.create(
                requestResponse -> {
                    return Single.just("hello" + requestResponse);
                } ,
                requestStream -> {
                    return just("a_" + requestStream, "b_" + requestStream);
                } , null, null);

        // start server with protocol
        HttpServer<ByteBuf, ByteBuf> server = HttpServer.newServer();
        int port = server.getServerPort();
        server.start((request, response) -> {
            return response.acceptWebSocketUpgrade(handler::acceptWebsocket);
        });

        // TODO send actual requests
        HttpClient.newClient("localhost", server.getServerPort())
                .createGet("/")
                .requestWebSocketUpgrade();

        server.shutdown();
    }
}
