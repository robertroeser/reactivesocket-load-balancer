package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.ClosedConnectionsProvider;
import io.reactivesocket.loadbalancer.SocketAddressFactory;
import io.reactivesocket.loadbalancer.XORShiftRandom;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by rroeser on 3/9/16.
 */
public class LoadBalancerDelegatingReactiveSocketTest {
    @Test
    public void testNoConnectionsAvailable() {
        LoadBalancerDelegatingReactiveSocket client
            = new LoadBalancerDelegatingReactiveSocket(new SocketAddressFactory() {
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
                        s.onComplete();
                    }
                };
            }
        }, new ReactiveSocketFactory<SocketAddress, DelegatingReactiveSocket>() {
            @Override
            public DelegatingReactiveSocket callAndWait(SocketAddress socketAddress) {
                return null;
            }

            @Override
            public Publisher<DelegatingReactiveSocket> call(SocketAddress socketAddress) {
                return null;
            }
        }, new LoadBalancerDelegatingReactiveSocket.NumberGenerator() {
            @Override
            public int generateInt() {
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

        DelegatingReactiveSocket c = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c.availability()).thenReturn(0.0);

        LoadBalancerDelegatingReactiveSocket client
            = new LoadBalancerDelegatingReactiveSocket(new SocketAddressFactory() {
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
                        s.onComplete();
                    }
                };
            }
        }, new ReactiveSocketFactory<SocketAddress, DelegatingReactiveSocket>() {
            @Override
            public Publisher<DelegatingReactiveSocket> call(SocketAddress socketAddress) {
                return s -> {
                    s.onSubscribe(EmptySubscription.INSTANCE);
                    s.onNext(c);
                    s.onComplete();
                };
            }
        }, new LoadBalancerDelegatingReactiveSocket.NumberGenerator() {
            @Override
            public int generateInt() {
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

        DelegatingReactiveSocket c = Mockito.mock(DelegatingReactiveSocket.class);
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

        LoadBalancerDelegatingReactiveSocket client
            = new LoadBalancerDelegatingReactiveSocket(new SocketAddressFactory() {
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
                        s.onComplete();
                    }
                };
            }
        }, new ReactiveSocketFactory<SocketAddress, DelegatingReactiveSocket>() {
            @Override
            public Publisher<DelegatingReactiveSocket> call(SocketAddress socketAddress) {
                return s -> {
                    s.onSubscribe(EmptySubscription.INSTANCE);
                    s.onNext(c);
                    s.onComplete();
                };
            }
        }, new LoadBalancerDelegatingReactiveSocket.NumberGenerator() {
            @Override
            public int generateInt() {
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

    @Test
    public void testTwoAvailibleConnection() {

        DelegatingReactiveSocket c1 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c1.availability()).thenReturn(0.5);
        Mockito
            .when(c1.requestResponse(Mockito.any(Payload.class)))
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

        DelegatingReactiveSocket c2 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c2.availability()).thenReturn(0.9);
        Mockito
            .when(c2.requestResponse(Mockito.any(Payload.class)))
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

        LoadBalancerDelegatingReactiveSocket client
            = new LoadBalancerDelegatingReactiveSocket(new SocketAddressFactory() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost1", 8080));
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost2", 8080));
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
                        s.onComplete();
                    }
                };
            }
        }, new ReactiveSocketFactory<SocketAddress, DelegatingReactiveSocket>() {
            @Override
            public Publisher<DelegatingReactiveSocket> call(SocketAddress socketAddress) {
                return s -> {
                    s.onSubscribe(EmptySubscription.INSTANCE);

                    InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
                    if (inetSocketAddress.getHostName().equals("localhost1")) {
                        s.onNext(c1);
                    } else {
                        s.onNext(c2);
                    }
                    s.onComplete();
                };
            }
        }, new LoadBalancerDelegatingReactiveSocket.NumberGenerator() {
            @Override
            public int generateInt() {
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

        Mockito.verify(c1, Mockito.times(0)).requestResponse(Mockito.any(Payload.class));
        Mockito.verify(c2, Mockito.times(1)).requestResponse(Mockito.any(Payload.class));
    }

    @Test
    public void testNAvailibleConnectionNoneAvailable() {

        DelegatingReactiveSocket c1 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c1.availability()).thenReturn(0.0);
        Mockito
            .when(c1.requestResponse(Mockito.any(Payload.class)))
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

        DelegatingReactiveSocket c2 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c2.availability()).thenReturn(0.0);
        Mockito
            .when(c2.requestResponse(Mockito.any(Payload.class)))
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

        DelegatingReactiveSocket c3 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c3.availability()).thenReturn(0.0);
        Mockito
            .when(c3.requestResponse(Mockito.any(Payload.class)))
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

        DelegatingReactiveSocket c4 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c4.availability()).thenReturn(0.0);
        Mockito
            .when(c4.requestResponse(Mockito.any(Payload.class)))
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

        LoadBalancerDelegatingReactiveSocket client
            = new LoadBalancerDelegatingReactiveSocket(new SocketAddressFactory() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost1", 8080));
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost2", 8080));
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost3", 8080));
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost4", 8080));
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
                        s.onComplete();
                    }
                };
            }
        },
        new ReactiveSocketFactory<SocketAddress, DelegatingReactiveSocket>() {
            @Override
            public Publisher<DelegatingReactiveSocket> call(SocketAddress socketAddress) {
                return s -> {
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
                };
            }

            }, new LoadBalancerDelegatingReactiveSocket.NumberGenerator() {
            @Override
            public int generateInt() {
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
    public void testAvailibleConnectionAvailable() {
        ClosedConnectionsProvider closedConnectionsProvider = new ClosedConnectionsProvider() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(new ArrayList<SocketAddress>());
                        s.onComplete();
                    }
                };
            }
        };

        DelegatingReactiveSocket c1 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c1.availability()).thenReturn(1.0);
        Mockito
            .when(c1.requestResponse(Mockito.any(Payload.class)))
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

        DelegatingReactiveSocket c2 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c2.availability()).thenReturn(1.0);
        Mockito
            .when(c2.requestResponse(Mockito.any(Payload.class)))
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

        for (int i = 0; i < 50; i++) {
            availibleConnections(c1, c2, closedConnectionsProvider);
        }

        Mockito.verify(c1, Mockito.atLeast(1)).requestResponse(Mockito.any(Payload.class));
        Mockito.verify(c2, Mockito.atLeast(1)).requestResponse(Mockito.any(Payload.class));
    }

    @Test
    public void testRemoveConnection() {
        AtomicBoolean tripped = new AtomicBoolean();
        ClosedConnectionsProvider closedConnectionsProvider = new ClosedConnectionsProvider() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        if (!tripped.get()) {
                            s.onNext(new ArrayList<SocketAddress>());
                        } else {
                            ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                            socketAddresses.add(InetSocketAddress.createUnresolved("localhost2", 8080));
                            s.onNext(socketAddresses);
                        }

                        s.onComplete();
                    }
                };
            }
        };

        AtomicInteger c1Count = new AtomicInteger();
        AtomicInteger c2Count = new AtomicInteger();
        DelegatingReactiveSocket c1 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c1.availability()).thenReturn(0.5);
        Mockito
            .when(c1.requestResponse(Mockito.any(Payload.class)))
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
                    c1Count.incrementAndGet();
                    s.onComplete();
                }
            });

        DelegatingReactiveSocket c2 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c2.availability()).thenReturn(1.0);
        Mockito
            .when(c2.requestResponse(Mockito.any(Payload.class)))
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
                    c2Count.incrementAndGet();
                    s.onComplete();
                }
            });

        for (int i = 0; i < 50; i++) {
            if (i == 5) {
                tripped.set(true);
            }

            availibleConnections(c1, c2, closedConnectionsProvider);
        }

        Mockito.verify(c1, Mockito.atLeast(1)).requestResponse(Mockito.any(Payload.class));
        Mockito.verify(c2, Mockito.atLeast(1)).requestResponse(Mockito.any(Payload.class));

        Assert.assertTrue(c1Count.get() < c2Count.get());

    }

    @Test
    public void testHigherAvailibleIsCalledMoreTimes() {
        ClosedConnectionsProvider closedConnectionsProvider = new ClosedConnectionsProvider() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(new ArrayList<SocketAddress>());
                        s.onComplete();
                    }
                };
            }
        };

        AtomicInteger c1Count = new AtomicInteger();
        AtomicInteger c2Count = new AtomicInteger();
        DelegatingReactiveSocket c1 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c1.availability()).thenReturn(1.0);
        Mockito
            .when(c1.requestResponse(Mockito.any(Payload.class)))
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
                    c1Count.incrementAndGet();
                    s.onComplete();
                }
            });

        DelegatingReactiveSocket c2 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c2.availability()).thenReturn(0.5);
        Mockito
            .when(c2.requestResponse(Mockito.any(Payload.class)))
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
                    c2Count.incrementAndGet();
                    s.onComplete();
                }
            });

        for (int i = 0; i < 50; i++) {
            availibleConnections(c1, c2, closedConnectionsProvider);
        }

        Mockito.verify(c1, Mockito.atLeast(1)).requestResponse(Mockito.any(Payload.class));
        Mockito.verify(c2, Mockito.atLeast(1)).requestResponse(Mockito.any(Payload.class));

        Assert.assertTrue(c1Count.get() > c2Count.get());

    }

    public void availibleConnections(DelegatingReactiveSocket c1, DelegatingReactiveSocket c2, ClosedConnectionsProvider closedConnectionsProvider) {

        DelegatingReactiveSocket c3 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c3.availability()).thenReturn(1.0);
        Mockito
            .when(c3.requestResponse(Mockito.any(Payload.class)))
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

        DelegatingReactiveSocket c4 = Mockito.mock(DelegatingReactiveSocket.class);
        Mockito.when(c4.availability()).thenReturn(0.0);
        Mockito
            .when(c4.requestResponse(Mockito.any(Payload.class)))
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

        LoadBalancerDelegatingReactiveSocket client
            = new LoadBalancerDelegatingReactiveSocket(new SocketAddressFactory() {
            @Override
            public Publisher<List<SocketAddress>> call() {
                return new Publisher<List<SocketAddress>>() {
                    @Override
                    public void subscribe(Subscriber<? super List<SocketAddress>> s) {
                        ArrayList<SocketAddress> socketAddresses = new ArrayList<>();
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost1", 8080));
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost2", 8080));
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost3", 8080));
                        socketAddresses.add(InetSocketAddress.createUnresolved("localhost4", 8080));
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(socketAddresses);
                    }
                };
            }
        }, closedConnectionsProvider, new ReactiveSocketFactory<SocketAddress, DelegatingReactiveSocket>() {
            @Override
            public Publisher<DelegatingReactiveSocket> call(SocketAddress socketAddress) {
                return s -> {
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
                };
            }
        }, new LoadBalancerDelegatingReactiveSocket.NumberGenerator() {
            @Override
            public int generateInt() {
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
    }
}