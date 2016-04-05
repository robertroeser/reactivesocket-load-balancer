/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.internal.rx.EmptySubscription;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class InitializingDelegatingReactiveSocketTest {

    @Mock
    ReactiveSocket reactiveSocket;

    @Mock
    ReactiveSocketFactory<SocketAddress, ReactiveSocket> reactiveSocketFactory;

    @Test
    public void testInitNewSocketClient() {
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

        SocketAddress socketAddress = InetSocketAddress.createUnresolved("localhost", 8080);

        Mockito.when(reactiveSocketFactory.call(Mockito.any(SocketAddress.class), Mockito.anyInt(), Mockito.any(TimeUnit.class), Mockito.any(ScheduledExecutorService.class))).thenReturn(new Publisher<ReactiveSocket>() {
            @Override
            public void subscribe(Subscriber<? super ReactiveSocket> s) {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(reactiveSocket);
                s.onComplete();
            }
        });

        InitializingDelegatingReactiveSocket initializingReactiveSocketClient
            = new InitializingDelegatingReactiveSocket(reactiveSocketFactory, socketAddress, 10, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);

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

        TestSubscriber<Payload> testSubscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();

        Mockito.verify(reactiveSocketFactory, Mockito.times(1)).call(Mockito.any(SocketAddress.class), Mockito.anyInt(), Mockito.any(TimeUnit.class), Mockito.any(ScheduledExecutorService.class));

        availability = initializingReactiveSocketClient.availability();
        System.out.println(availability);
        Assert.assertTrue(1.0 == availability);
    }

    @Test
    public void testInitNewClientWithExceptionInFactory() {
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

        SocketAddress socketAddress = InetSocketAddress.createUnresolved("localhost", 8080);

        Mockito.when(reactiveSocketFactory.call(Mockito.any(SocketAddress.class), Mockito.anyInt(), Mockito.any(TimeUnit.class), Mockito.any(ScheduledExecutorService.class))).thenReturn(new Publisher<ReactiveSocket>() {
            @Override
            public void subscribe(Subscriber<? super ReactiveSocket> s) {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onError(new RuntimeException());
                s.onComplete();
            }
        });

        InitializingDelegatingReactiveSocket initializingReactiveSocketClient
            = new InitializingDelegatingReactiveSocket(reactiveSocketFactory, socketAddress, 10, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);

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

        TestSubscriber<Payload> testSubscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).doOnError(Throwable::printStackTrace).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(RuntimeException.class);

        Mockito.verify(reactiveSocketFactory, Mockito.times(1)).call(Mockito.any(SocketAddress.class), Mockito.anyInt(), Mockito.any(TimeUnit.class), Mockito.any(ScheduledExecutorService.class));

        availability = initializingReactiveSocketClient.availability();
        System.out.println(availability);
        Assert.assertTrue(0.0 == availability);

    }

    @Test
    public void testInitNewClientWithExceptionInReactiveSocket() {
        Mockito.when(reactiveSocket.availability()).thenReturn(0.5);

        Mockito.when(reactiveSocket.requestResponse(Mockito.any(Payload.class))).thenReturn(new Publisher<Payload>() {
            @Override
            public void subscribe(Subscriber<? super Payload> s) {
                s.onError(new RuntimeException());
                s.onComplete();
            }
        });

        SocketAddress socketAddress = InetSocketAddress.createUnresolved("localhost", 8080);

        Mockito.when(reactiveSocketFactory.call(Mockito.any(SocketAddress.class), Mockito.anyInt(), Mockito.any(TimeUnit.class), Mockito.any(ScheduledExecutorService.class))).thenReturn(new Publisher<ReactiveSocket>() {
            @Override
            public void subscribe(Subscriber<? super ReactiveSocket> s) {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(reactiveSocket);
                s.onComplete();
            }
        });

        InitializingDelegatingReactiveSocket initializingReactiveSocketClient
            = new InitializingDelegatingReactiveSocket(reactiveSocketFactory, socketAddress, 10, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);

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

        TestSubscriber<Payload> testSubscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).doOnError(Throwable::printStackTrace).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(RuntimeException.class);

        Mockito.verify(reactiveSocketFactory, Mockito.times(1)).call(Mockito.any(SocketAddress.class), Mockito.anyInt(), Mockito.any(TimeUnit.class), Mockito.any(ScheduledExecutorService.class));

        availability = initializingReactiveSocketClient.availability();
        System.out.println(availability);
        Assert.assertTrue(1.0 == availability);

    }

    @Test
    public void testInitNewClientWithExceptionInFactoryAndThenWaitForRetryWindow() throws Exception {
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

        SocketAddress socketAddress = InetSocketAddress.createUnresolved("localhost", 8080);

        Mockito.when(reactiveSocketFactory.call(Mockito.any(SocketAddress.class), Mockito.anyInt(), Mockito.any(TimeUnit.class), Mockito.any(ScheduledExecutorService.class))).thenReturn(new Publisher<ReactiveSocket>() {
            @Override
            public void subscribe(Subscriber<? super ReactiveSocket> s) {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onError(new RuntimeException());
                s.onComplete();
            }
        });

        InitializingDelegatingReactiveSocket initializingReactiveSocketClient
            = new InitializingDelegatingReactiveSocket(reactiveSocketFactory, socketAddress, 10, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

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

        TestSubscriber<Payload> testSubscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).doOnError(Throwable::printStackTrace).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(RuntimeException.class);

        Mockito.verify(reactiveSocketFactory, Mockito.times(1)).call(Mockito.any(SocketAddress.class), Mockito.anyInt(), Mockito.any(TimeUnit.class), Mockito.any(ScheduledExecutorService.class));

        availability = initializingReactiveSocketClient.availability();
        System.out.println(availability);
        Assert.assertTrue(0.0 == availability);

        Thread.sleep(1_001);
        availability = initializingReactiveSocketClient.availability();
        System.out.println(availability);
        Assert.assertTrue(1.0 == availability);

    }

    @Test
    public void testWaitingForSocketToBeCreated() throws Exception {

        CountDownLatch latch = new CountDownLatch(2);

        Mockito.when(reactiveSocket.availability()).thenReturn(0.5);

        Mockito.when(reactiveSocket.requestResponse(Mockito.any(Payload.class))).thenReturn(new Publisher<Payload>() {
            @Override
            public void subscribe(Subscriber<? super Payload> s) {
                latch.countDown();
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

        SocketAddress socketAddress = InetSocketAddress.createUnresolved("localhost", 8080);

        Mockito.when(reactiveSocketFactory.call(Mockito.any(SocketAddress.class), Mockito.anyInt(), Mockito.any(TimeUnit.class), Mockito.any(ScheduledExecutorService.class))).thenReturn(new Publisher<ReactiveSocket>() {
            @Override
            public void subscribe(Subscriber<? super ReactiveSocket> s) {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(reactiveSocket);
                s.onComplete();
            }
        });

        InitializingDelegatingReactiveSocket initializingReactiveSocketClient
            = new InitializingDelegatingReactiveSocket(reactiveSocketFactory, socketAddress, 10, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);

        initializingReactiveSocketClient.guard.tryAcquire();

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

        RxReactiveStreams
            .toObservable(payloadPublisher)
            .subscribeOn(Schedulers.computation())
            .subscribe();

        Thread.sleep(250);

        Assert.assertFalse(initializingReactiveSocketClient.awaitingReactiveSocket.isEmpty());
        Assert.assertEquals(2, latch.getCount());

        initializingReactiveSocketClient.guard.release();

        Publisher<Payload> payloadPublisher2 = initializingReactiveSocketClient.requestResponse(new Payload() {
            @Override
            public ByteBuffer getData() {
                return null;
            }

            @Override
            public ByteBuffer getMetadata() {
                return null;
            }
        });

        RxReactiveStreams.toObservable(payloadPublisher2).subscribe();

        latch.await(1, TimeUnit.SECONDS);

    }

}