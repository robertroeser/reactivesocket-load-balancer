package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.exceptions.SetupException;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.XORShiftRandom;
import io.reactivesocket.local.LocalClientReactiveSocketFactory;
import io.reactivesocket.local.LocalServerReactiveSocketFactory;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by rroeser on 4/7/16.
 */
@Ignore
public class LoadBalanceMultipleSocketsTest {

    private static final String LOCAL_SOCKET_NAME = "localSocketName";

    private static LoadBalancerDelegatingReactiveSocket<SocketAddress> client;

    @BeforeClass
    public static void setup() {
        client = new LoadBalancerDelegatingReactiveSocket<>(
            () -> s -> {
                ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost1", 8080));
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost2", 8080));
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost3", 8080));
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost4", 8080));
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(socketAddresses);
                s.onComplete();
            },
            () -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(new ArrayList<SocketAddress>());
                s.onComplete();
            },
            socketAddress -> s -> {
                Publisher<ReactiveSocket> call = LocalClientReactiveSocketFactory
                    .INSTANCE
                    .call(new LocalClientReactiveSocketFactory.Config(LOCAL_SOCKET_NAME, "UTF-8", "UTF-8"));

                call.subscribe(new Subscriber<ReactiveSocket>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(ReactiveSocket reactiveSocket) {
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(reactiveSocket);
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace();
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        s.onComplete();
                    }
                });
            },
            () -> XORShiftRandom.getInstance().randomInt()
        );

        ExecutorService executorService = Executors.newScheduledThreadPool(4);

        LocalServerReactiveSocketFactory
            .INSTANCE
            .call(new LocalServerReactiveSocketFactory.Config(LOCAL_SOCKET_NAME, new ConnectionSetupHandler() {
                @Override
                public RequestHandler apply(ConnectionSetupPayload setupPayload, ReactiveSocket rs) throws SetupException {
                    return new RequestHandler() {
                        @Override
                        public Publisher<Payload> handleRequestResponse(Payload payload) {
                            return s -> {
                                executorService.execute(() -> {
                                    Payload p = new Payload() {
                                        @Override
                                        public ByteBuffer getData() {
                                            return ByteBuffer.wrap("hello".getBytes());
                                        }

                                        @Override
                                        public ByteBuffer getMetadata() {
                                            return ByteBuffer.allocate(0);
                                        }
                                    };
                                    s.onSubscribe(EmptySubscription.INSTANCE);
                                    s.onNext(p);
                                    s.onComplete();
                                });
                            };
                        }

                        @Override
                        public Publisher<Payload> handleRequestStream(Payload payload) {
                            return null;
                        }

                        @Override
                        public Publisher<Payload> handleSubscription(Payload payload) {
                            return null;
                        }

                        @Override
                        public Publisher<Void> handleFireAndForget(Payload payload) {
                            return null;
                        }

                        @Override
                        public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> inputs) {
                            return null;
                        }

                        @Override
                        public Publisher<Void> handleMetadataPush(Payload payload) {
                            return null;
                        }
                    };
                }
            }));

    }
}
