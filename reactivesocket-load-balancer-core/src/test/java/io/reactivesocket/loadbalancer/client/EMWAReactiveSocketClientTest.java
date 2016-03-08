package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by rroeser on 3/7/16.
 */
public class EMWAReactiveSocketClientTest {
    @Test
    public void testStartupPenalty() {
        EMWAReactiveSocketClient client = new EMWAReactiveSocketClient(new ReactiveSocketClient() {
            @Override
            public Publisher<Payload> requestResponse(Payload payload) {
                return new Publisher<Payload>() {
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
                };
            }

            @Override
            public void close() throws Exception {

            }
        }, 2, 5);

        double availability = client.availability();
        Assert.assertTrue(1.0 == availability);
        client.pending = 1;
        availability = client.availability();
        Assert.assertTrue(1.0 > availability);
    }

    @Test
    public void testError() {
        AtomicInteger count = new AtomicInteger(0);
        EMWAReactiveSocketClient client = new EMWAReactiveSocketClient(new ReactiveSocketClient() {
            @Override
            public Publisher<Payload> requestResponse(Payload payload) {
                return s -> {
                    if (count.get() < 1) {

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

                        count.incrementAndGet();
                        s.onComplete();
                    } else {
                        s.onError(new RuntimeException());
                    }
                };
            }

            @Override
            public void close() throws Exception {

            }
        }, 2, 5);

        double availability = client.availability();
        Assert.assertTrue(1.0 == availability);

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


        TestSubscriber subscriber = new TestSubscriber();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertCompleted();
        double good = client.availability();

        subscriber = new TestSubscriber();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertError(RuntimeException.class);
        double bad = client.availability();
        Assert.assertTrue(good > bad);

    }

    @Test
    public void testGoodRequest() {
        AtomicInteger integer = new AtomicInteger(2500);
        EMWAReactiveSocketClient client = new EMWAReactiveSocketClient(new ReactiveSocketClient() {
            @Override
            public Publisher<Payload> requestResponse(Payload payload) {
                return s -> {
                    try {
                        Thread.sleep(50 + integer.getAndAdd(-500));
                    } catch (Throwable t) {}

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
                };

            }

            @Override
            public void close() throws Exception {

            }
        }, 2, 5);

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

        TestSubscriber subscriber = new TestSubscriber();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertCompleted();

        double old = client.availability();

        for (int i = 0; i < 0; i++) {
            subscriber = new TestSubscriber();
            RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
            subscriber.awaitTerminalEvent();
            subscriber.assertCompleted();
            double n = client.availability();
            System.out.println();
            Assert.assertTrue(old < n);
            old = n;
        }
    }

    @Test
    public void testIncreasingSleep() {
        AtomicInteger integer = new AtomicInteger();
        EMWAReactiveSocketClient client = new EMWAReactiveSocketClient(new ReactiveSocketClient() {
            @Override
            public Publisher<Payload> requestResponse(Payload payload) {
                return s -> {
                    try {
                        Thread.sleep(50 + integer.getAndAdd(500));
                    } catch (Throwable t) {}

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
                };

            }

            @Override
            public void close() throws Exception {

            }
        }, 2, 5);

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

        TestSubscriber subscriber = new TestSubscriber();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertCompleted();

        double old = client.availability();

        for (int i = 0; i < 5; i++) {
            subscriber = new TestSubscriber();
            RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
            subscriber.awaitTerminalEvent();
            subscriber.assertCompleted();
            double n = client.availability();
            System.out.println(old);
            System.out.println(n);
            System.out.println();
            //Assert.assertTrue(old < n);
            old = n;
        }
    }

}