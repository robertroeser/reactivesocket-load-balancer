package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.ReactiveSocketFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 3/8/16.
 */
public class InitializingReactiveSocketClientTest {
    @Test
    public void testInitNewSocketClient() {
        ReactiveSocket reactiveSocket = Mockito.mock(ReactiveSocket.class);
        Mockito.when(reactiveSocket.availability()).thenReturn(0.5);

        Mockito.when(reactiveSocket.requestResponse(Mockito.any(Payload.class))).thenReturn(new Publisher<Payload>() {
            @Override
            public void subscribe(Subscriber<? super Payload> s) {
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

        ReactiveSocketFactory reactiveSocketFactory = Mockito.mock(ReactiveSocketFactory.class);

        SocketAddress socketAddress = InetSocketAddress.createUnresolved("localhost", 8080);

        Mockito.when(reactiveSocketFactory.call(socketAddress, 10, TimeUnit.SECONDS)).thenReturn(new Publisher<ReactiveSocket>() {
            @Override
            public void subscribe(Subscriber<? super ReactiveSocket> s) {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(reactiveSocket);
                s.onComplete();
            }
        });

        InitializingReactiveSocketClient initializingReactiveSocketClient
            = new InitializingReactiveSocketClient(reactiveSocketFactory, socketAddress, 10, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);

        double availability = initializingReactiveSocketClient.availability();
        Assert.assertTrue(1.0 == availability);

        Publisher<Payload> payloadPublisher = initializingReactiveSocketClient.requestResponse(new Payload() {
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

        Mockito.verify(reactiveSocketFactory, Mockito.times(1)).call(socketAddress, 10, TimeUnit.SECONDS);

        availability = initializingReactiveSocketClient.availability();
        Assert.assertTrue(0.5 == availability);
    }

    @Test
    public void testInitNewClientWithExceptionInFactory() {
        ReactiveSocket reactiveSocket = Mockito.mock(ReactiveSocket.class);
        Mockito.when(reactiveSocket.availability()).thenReturn(0.5);

        Mockito.when(reactiveSocket.requestResponse(Mockito.any(Payload.class))).thenReturn(new Publisher<Payload>() {
            @Override
            public void subscribe(Subscriber<? super Payload> s) {
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

        ReactiveSocketFactory reactiveSocketFactory = Mockito.mock(ReactiveSocketFactory.class);

        SocketAddress socketAddress = InetSocketAddress.createUnresolved("localhost", 8080);

        Mockito.when(reactiveSocketFactory.call(socketAddress, 10, TimeUnit.SECONDS)).thenReturn(new Publisher<ReactiveSocket>() {
            @Override
            public void subscribe(Subscriber<? super ReactiveSocket> s) {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onError(new RuntimeException());
                s.onComplete();
            }
        });

        InitializingReactiveSocketClient initializingReactiveSocketClient
            = new InitializingReactiveSocketClient(reactiveSocketFactory, socketAddress, 10, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);

        double availability = initializingReactiveSocketClient.availability();
        Assert.assertTrue(1.0 == availability);

        Publisher<Payload> payloadPublisher = initializingReactiveSocketClient.requestResponse(new Payload() {
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
        RxReactiveStreams.toObservable(payloadPublisher).doOnError(Throwable::printStackTrace).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(RuntimeException.class);

        Mockito.verify(reactiveSocketFactory, Mockito.times(1)).call(socketAddress, 10, TimeUnit.SECONDS);

        availability = initializingReactiveSocketClient.availability();
        System.out.println(availability);
        Assert.assertTrue(0.0 == availability);

    }

    @Test
    public void testInitNewClientWithExceptionInReactiveSocket() {
        ReactiveSocket reactiveSocket = Mockito.mock(ReactiveSocket.class);
        Mockito.when(reactiveSocket.availability()).thenReturn(0.5);

        Mockito.when(reactiveSocket.requestResponse(Mockito.any(Payload.class))).thenReturn(new Publisher<Payload>() {
            @Override
            public void subscribe(Subscriber<? super Payload> s) {
                s.onError(new RuntimeException());
                s.onComplete();
            }
        });

        ReactiveSocketFactory reactiveSocketFactory = Mockito.mock(ReactiveSocketFactory.class);

        SocketAddress socketAddress = InetSocketAddress.createUnresolved("localhost", 8080);

        Mockito.when(reactiveSocketFactory.call(socketAddress, 10, TimeUnit.SECONDS)).thenReturn(new Publisher<ReactiveSocket>() {
            @Override
            public void subscribe(Subscriber<? super ReactiveSocket> s) {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(reactiveSocket);
                s.onComplete();
            }
        });

        InitializingReactiveSocketClient initializingReactiveSocketClient
            = new InitializingReactiveSocketClient(reactiveSocketFactory, socketAddress, 10, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);

        double availability = initializingReactiveSocketClient.availability();
        Assert.assertTrue(1.0 == availability);

        Publisher<Payload> payloadPublisher = initializingReactiveSocketClient.requestResponse(new Payload() {
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
        RxReactiveStreams.toObservable(payloadPublisher).doOnError(Throwable::printStackTrace).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(RuntimeException.class);

        Mockito.verify(reactiveSocketFactory, Mockito.times(1)).call(socketAddress, 10, TimeUnit.SECONDS);

        availability = initializingReactiveSocketClient.availability();
        System.out.println(availability);
        Assert.assertTrue(0.5 == availability);

    }

    @Test
    public void testInitNewClientWithExceptionInFactoryAndThenWaitForRetryWindow() throws Exception {
        ReactiveSocket reactiveSocket = Mockito.mock(ReactiveSocket.class);
        Mockito.when(reactiveSocket.availability()).thenReturn(0.5);

        Mockito.when(reactiveSocket.requestResponse(Mockito.any(Payload.class))).thenReturn(new Publisher<Payload>() {
            @Override
            public void subscribe(Subscriber<? super Payload> s) {
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

        ReactiveSocketFactory reactiveSocketFactory = Mockito.mock(ReactiveSocketFactory.class);

        SocketAddress socketAddress = InetSocketAddress.createUnresolved("localhost", 8080);

        Mockito.when(reactiveSocketFactory.call(socketAddress, 10, TimeUnit.SECONDS)).thenReturn(new Publisher<ReactiveSocket>() {
            @Override
            public void subscribe(Subscriber<? super ReactiveSocket> s) {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onError(new RuntimeException());
                s.onComplete();
            }
        });

        InitializingReactiveSocketClient initializingReactiveSocketClient
            = new InitializingReactiveSocketClient(reactiveSocketFactory, socketAddress, 10, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

        double availability = initializingReactiveSocketClient.availability();
        Assert.assertTrue(1.0 == availability);

        Publisher<Payload> payloadPublisher = initializingReactiveSocketClient.requestResponse(new Payload() {
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
        RxReactiveStreams.toObservable(payloadPublisher).doOnError(Throwable::printStackTrace).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(RuntimeException.class);

        Mockito.verify(reactiveSocketFactory, Mockito.times(1)).call(socketAddress, 10, TimeUnit.SECONDS);

        availability = initializingReactiveSocketClient.availability();
        System.out.println(availability);
        Assert.assertTrue(0.0 == availability);

        Thread.sleep(1_001);
        availability = initializingReactiveSocketClient.availability();
        System.out.println(availability);
        Assert.assertTrue(1.0 == availability);

    }

}