package io.reactivesocket.websocket.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.exceptions.SetupException;
import io.reactivesocket.websocket.rxnetty.server.ReactiveSocketWebSocketServer;
import io.reactivex.netty.protocol.http.server.HttpServer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.RxReactiveStreams;

import java.nio.ByteBuffer;
import java.util.Random;

public class Pong {
    public static void main(String... args) {
        byte[] response = new byte[1024];
        //byte[] response = new byte[1024];
        Random r = new Random();
        r.nextBytes(response);

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
                                    Payload responsePayload = new Payload() {
                                        ByteBuffer data = ByteBuffer.wrap(response);
                                        ByteBuffer metadata = ByteBuffer.allocate(0);

                                        public ByteBuffer getData() {
                                            return data;
                                        }

                                        @Override
                                        public ByteBuffer getMetadata() {
                                            return metadata;
                                        }
                                    };

                                    s.onNext(responsePayload);
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
                        public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads) {
                            return null;
                        }

                        @Override
                        public Publisher<Void> handleMetadataPush(Payload payload) {
                            return null;
                        }
                    };
                }
            });

        HttpServer<ByteBuf, ByteBuf> server = HttpServer.newServer(8888)
//				.clientChannelOption(ChannelOption.AUTO_READ, true)
//            .enableWireLogging(LogLevel.ERROR)
            .start((req, resp) -> {
                return resp.acceptWebSocketUpgrade(serverHandler::acceptWebsocket);
            });

        server.awaitShutdown();
    }
}
