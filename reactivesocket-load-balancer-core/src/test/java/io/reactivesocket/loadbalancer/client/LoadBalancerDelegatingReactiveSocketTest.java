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
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.XORShiftRandom;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by rroeser on 3/9/16.
 */
public class LoadBalancerDelegatingReactiveSocketTest {
    private Payload dummyPayload = new Payload() {
        @Override
        public ByteBuffer getData() {
            return null;
        }

        @Override
        public ByteBuffer getMetadata() {
            return null;
        }
    };

    @Test
    public void testNoConnectionsAvailable() {
        LoadBalancerDelegatingReactiveSocket<SocketAddress> client =
            new LoadBalancerDelegatingReactiveSocket<>(() -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(new ArrayList<SocketAddress>());
            }, () -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(new ArrayList<SocketAddress>());
                s.onComplete();
            },
            socketAddress -> null,
            () -> XORShiftRandom.getInstance().randomInt());

        Publisher<Payload> payloadPublisher = client.requestResponse(dummyPayload);

        TestSubscriber<Payload> testSubscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(NoAvailableReactiveSocketClientsException.class);
    }

    @Test
    public void testNoConnectionsAvailableWithZeroAvailability() {

        ReactiveSocket c = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c.availability()).thenReturn(0.0);

        LoadBalancerDelegatingReactiveSocket<SocketAddress> client =
            new LoadBalancerDelegatingReactiveSocket<>(() -> s -> {
                ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost", 8080));
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(socketAddresses);
            }, () -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(new ArrayList<SocketAddress>());
                s.onComplete();
            }, socketAddress -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(c);
                s.onComplete();
            }, () -> XORShiftRandom.getInstance().randomInt());

        Publisher<Payload> payloadPublisher = client.requestResponse(dummyPayload);

        TestSubscriber<Payload> testSubscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(NoAvailableReactiveSocketClientsException.class);

        Mockito.verify(c, Mockito.times(1)).availability();
    }

    @Test
    public void testOneAvailibleConnection() {

        ReactiveSocket c = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c.availability()).thenReturn(1.0);
        Mockito
            .when(c.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                s.onComplete();
            });

        LoadBalancerDelegatingReactiveSocket<SocketAddress> client =
            new LoadBalancerDelegatingReactiveSocket<>(() -> s -> {
                ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost", 8080));
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(socketAddresses);
            }, () -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(new ArrayList<SocketAddress>());
                s.onComplete();
            }, socketAddress -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(c);
                s.onComplete();
            }, () -> XORShiftRandom.getInstance().randomInt());

        Publisher<Payload> payloadPublisher = client.requestResponse(dummyPayload);

        TestSubscriber<Payload> testSubscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();

        Mockito.verify(c, Mockito.times(1)).requestResponse(Mockito.any(Payload.class));
    }

    @Test
    public void testTwoAvailibleConnection() {

        ReactiveSocket c1 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c1.availability()).thenReturn(0.5);
        Mockito
            .when(c1.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                s.onComplete();
            });

        ReactiveSocket c2 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c2.availability()).thenReturn(0.9);
        Mockito
            .when(c2.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                s.onComplete();
            });

        LoadBalancerDelegatingReactiveSocket<SocketAddress> client =
            new LoadBalancerDelegatingReactiveSocket<>(() -> s -> {
                ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost1", 8080));
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost2", 8080));
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(socketAddresses);
            }, () -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(new ArrayList<SocketAddress>());
                s.onComplete();
            }, socketAddress -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);

                InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
                if (inetSocketAddress.getHostName().equals("localhost1")) {
                    s.onNext(c1);
                } else {
                    s.onNext(c2);
                }
                s.onComplete();
            }, () -> XORShiftRandom.getInstance().randomInt());

        Publisher<Payload> payloadPublisher = client.requestResponse(dummyPayload);

        TestSubscriber<Payload> testSubscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();

        Mockito.verify(c1, Mockito.times(0)).requestResponse(Mockito.any(Payload.class));
        Mockito.verify(c2, Mockito.times(1)).requestResponse(Mockito.any(Payload.class));
    }

    @Test
    public void testNAvailibleConnectionNoneAvailable() {

        ReactiveSocket c1 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c1.availability()).thenReturn(0.0);
        Mockito
            .when(c1.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                s.onComplete();
            });

        ReactiveSocket c2 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c2.availability()).thenReturn(0.0);
        Mockito
            .when(c2.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                s.onComplete();
            });

        ReactiveSocket c3 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c3.availability()).thenReturn(0.0);
        Mockito
            .when(c3.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                s.onComplete();
            });

        ReactiveSocket c4 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c4.availability()).thenReturn(0.0);
        Mockito
            .when(c4.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                s.onComplete();
            });

        LoadBalancerDelegatingReactiveSocket<SocketAddress> client =
            new LoadBalancerDelegatingReactiveSocket<>(() -> s -> {
                ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost1", 8080));
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost2", 8080));
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost3", 8080));
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost4", 8080));
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(socketAddresses);
            }, () -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(new ArrayList<SocketAddress>());
                s.onComplete();
            }, socketAddress -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);

                InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
                if (inetSocketAddress.getHostName().equals("localhost1")) {
                    s.onNext(c1);
                } else if (inetSocketAddress.getHostName().equals("localhost2")) {
                    s.onNext(c2);
                } else if (inetSocketAddress.getHostName().equals("localhost3")) {
                    s.onNext(c3);
                } else {
                    s.onNext(c4);
                }
                s.onComplete();
            }, () -> XORShiftRandom.getInstance().randomInt());

        Publisher<Payload> payloadPublisher = client.requestResponse(dummyPayload);

        TestSubscriber<Payload> testSubscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(NoAvailableReactiveSocketClientsException.class);

    }

    @Test
    public void testAvailibleConnectionAvailable() {
        Publisher<List<SocketAddress>> closedConnectionsProvider = s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            s.onNext(new ArrayList<SocketAddress>());
            s.onComplete();
        };

        ReactiveSocket c1 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c1.availability()).thenReturn(1.0);
        Mockito
            .when(c1.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                s.onComplete();
            });

        ReactiveSocket c2 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c2.availability()).thenReturn(1.0);
        Mockito
            .when(c2.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                s.onComplete();
            });

        for (int i = 0; i < 50; i++) {
            availableConnections(c1, c2, closedConnectionsProvider);
        }

        Mockito.verify(c1, Mockito.atLeast(1)).requestResponse(Mockito.any(Payload.class));
        Mockito.verify(c2, Mockito.atLeast(1)).requestResponse(Mockito.any(Payload.class));
    }

    @Test
    public void testRemoveConnection() {
        AtomicBoolean tripped = new AtomicBoolean();
        Publisher<List<SocketAddress>> closedConnectionsProvider = s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            if (!tripped.get()) {
                s.onNext(new ArrayList<SocketAddress>());
            } else {
                ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost2", 8080));
                s.onNext(socketAddresses);
            }

            s.onComplete();
        };

        AtomicInteger c1Count = new AtomicInteger();
        AtomicInteger c2Count = new AtomicInteger();
        ReactiveSocket c1 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c1.availability()).thenReturn(0.5);
        Mockito
            .when(c1.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                c1Count.incrementAndGet();
                s.onComplete();
            });

        ReactiveSocket c2 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c2.availability()).thenReturn(1.0);
        Mockito
            .when(c2.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                c2Count.incrementAndGet();
                s.onComplete();
            });

        for (int i = 0; i < 50; i++) {
            if (i == 5) {
                tripped.set(true);
            }

            availableConnections(c1, c2, closedConnectionsProvider);
        }

        Mockito.verify(c1, Mockito.never()).requestResponse(Mockito.any(Payload.class));
        Mockito.verify(c2, Mockito.atLeast(1)).requestResponse(Mockito.any(Payload.class));

        Assert.assertTrue(c1Count.get() < c2Count.get());

    }

    @Test
    public void testHigherAvailableIsCalledMoreTimes() {
        Publisher<List<SocketAddress>> closedConnectionsProvider = s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            s.onNext(new ArrayList<SocketAddress>());
            s.onComplete();
        };

        AtomicInteger c1Count = new AtomicInteger();
        AtomicInteger c2Count = new AtomicInteger();
        ReactiveSocket c1 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c1.availability()).thenReturn(1.0);
        Mockito
            .when(c1.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                c1Count.incrementAndGet();
                s.onComplete();
            });

        ReactiveSocket c2 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c2.availability()).thenReturn(0.5);
        Mockito
            .when(c2.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                c2Count.incrementAndGet();
                s.onComplete();
            });

        for (int i = 0; i < 50; i++) {
            availableConnections(c1, c2, closedConnectionsProvider);
        }

        Mockito.verify(c1, Mockito.atLeast(1)).requestResponse(Mockito.any(Payload.class));
        Mockito.verify(c2, Mockito.atLeast(0)).requestResponse(Mockito.any(Payload.class));

        Assert.assertTrue(c1Count.get() > c2Count.get());

    }

    public void availableConnections(ReactiveSocket c1, ReactiveSocket c2, Publisher<List<SocketAddress>> closedConnectionsProvider) {

        ReactiveSocket c3 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c3.availability()).thenReturn(1.0);
        Mockito
            .when(c3.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                s.onComplete();
            });

        ReactiveSocket c4 = Mockito.mock(ReactiveSocket.class);
        Mockito.when(c4.availability()).thenReturn(0.0);
        Mockito
            .when(c4.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(dummyPayload);
                s.onComplete();
            });

        LoadBalancerDelegatingReactiveSocket<SocketAddress> client
            = new LoadBalancerDelegatingReactiveSocket<>(
            () -> s -> {
                ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost1", 8080));
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost2", 8080));
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost3", 8080));
                socketAddresses.add(InetSocketAddress.createUnresolved("localhost4", 8080));
                s.onSubscribe(EmptySubscription.INSTANCE);
                s.onNext(socketAddresses);
            },
            () -> closedConnectionsProvider,
            socketAddress -> s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
                if (inetSocketAddress.getHostName().equals("localhost1")) {
                    s.onNext(c1);
                } else if (inetSocketAddress.getHostName().equals("localhost2")) {
                    s.onNext(c2);
                } else if (inetSocketAddress.getHostName().equals("localhost3")) {
                    s.onNext(c3);
                } else {
                    s.onNext(c4);
                }
                s.onComplete();
            },
            () -> XORShiftRandom.getInstance().randomInt());

        Publisher<Payload> payloadPublisher = client.requestResponse(dummyPayload);

        TestSubscriber<Payload> testSubscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();
    }
}