package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.ClosedConnectionsProvider;
import io.reactivesocket.loadbalancer.ReactiveSocketClientFactory;
import io.reactivesocket.loadbalancer.SocketAddressFactory;
import io.reactivesocket.loadbalancer.XORShiftRandom;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rroeser on 3/9/16.
 */
public class LoadBalancerReactiveSocketClientTest {
    @Test
    public void testNoConnectionsAvailable() {
        LoadBalancerReactiveSocketClient client
            = new LoadBalancerReactiveSocketClient(new SocketAddressFactory() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(new ArrayList<SocketAddress>());
                    }
                };
            }
        }, new ClosedConnectionsProvider() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(new ArrayList<SocketAddress>());
                    }
                };
            }
        }, new ReactiveSocketClientFactory<SocketAddress>() {
            @Override
            public ReactiveSocketClient apply(SocketAddress socketAddress) {
                return null;
            }
        }, new LoadBalancerReactiveSocketClient.NumberGenerator() {
            @Override
            public int nextInt() {
                return XORShiftRandom.getInstance().randomInt();
            }
        });

        Publisher<Payload> payloadPublisher = client.requestResponse(new Payload() {
            @Override
            public ByteBuffer getData() {
                return null;
            }

            @Override
            public ByteBuffer getMetadata() {
                return null;
            }
        });

        TestSubscriber testSubscriber = new TestSubscriber();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(NoAvailableReactiveSocketClientsException.class);
    }

    @Test
    public void testNoConnectionsAvailableWithZeroAvailibility() {

        ReactiveSocketClient c = Mockito.mock(ReactiveSocketClient.class);
        Mockito.when(c.availability()).thenReturn(0.0);

        LoadBalancerReactiveSocketClient client
            = new LoadBalancerReactiveSocketClient(new SocketAddressFactory() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost", 8080));
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(socketAddresses);
                    }
                };
            }
        }, new ClosedConnectionsProvider() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(new ArrayList<SocketAddress>());
                    }
                };
            }
        }, new ReactiveSocketClientFactory<SocketAddress>() {
            @Override
            public ReactiveSocketClient apply(SocketAddress socketAddress) {

                return c;
            }
        }, new LoadBalancerReactiveSocketClient.NumberGenerator() {
            @Override
            public int nextInt() {
                return XORShiftRandom.getInstance().randomInt();
            }
        });

        Publisher<Payload> payloadPublisher = client.requestResponse(new Payload() {
            @Override
            public ByteBuffer getData() {
                return null;
            }

            @Override
            public ByteBuffer getMetadata() {
                return null;
            }
        });

        TestSubscriber testSubscriber = new TestSubscriber();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(NoAvailableReactiveSocketClientsException.class);

        Mockito.verify(c, Mockito.times(1)).availability();
    }

    @Test
    public void testOneAvailibleConnection() {

        ReactiveSocketClient c = Mockito.mock(ReactiveSocketClient.class);
        Mockito.when(c.availability()).thenReturn(1.0);
        Mockito
            .when(c.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(new Publisher<Payload>() {
                @Override
                public void subscribe(Subscriber<? super Payload> s) {
                    s.onSubscribe(EmptySubscription.INSTANCE);
                    s.onNext(new Payload() {
                        @Override
                        public ByteBuffer getData() {
                            return null;
                        }

                        @Override
                        public ByteBuffer getMetadata() {
                            return null;
                        }
                    });
                    s.onComplete();
                }
            });

        LoadBalancerReactiveSocketClient client
            = new LoadBalancerReactiveSocketClient(new SocketAddressFactory() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost", 8080));
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(socketAddresses);
                    }
                };
            }
        }, new ClosedConnectionsProvider() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(new ArrayList<SocketAddress>());
                    }
                };
            }
        }, new ReactiveSocketClientFactory<SocketAddress>() {
            @Override
            public ReactiveSocketClient apply(SocketAddress socketAddress) {

                return c;
            }
        }, new LoadBalancerReactiveSocketClient.NumberGenerator() {
            @Override
            public int nextInt() {
                return XORShiftRandom.getInstance().randomInt();
            }
        });

        Publisher<Payload> payloadPublisher = client.requestResponse(new Payload() {
            @Override
            public ByteBuffer getData() {
                return null;
            }

            @Override
            public ByteBuffer getMetadata() {
                return null;
            }
        });

        TestSubscriber testSubscriber = new TestSubscriber();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();

        Mockito.verify(c, Mockito.times(1)).requestResponse(Mockito.any(Payload.class));
    }
}