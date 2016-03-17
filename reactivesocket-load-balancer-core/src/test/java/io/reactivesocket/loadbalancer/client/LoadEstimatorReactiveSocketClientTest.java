package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import io.reactivesocket.internal.rx.EmptySubscription;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by rroeser on 3/7/16.
 */
public class LoadEstimatorReactiveSocketClientTest {
    @Test
    public void testStartupPenalty() {
        LoadEstimatorReactiveSocketClient client = new LoadEstimatorReactiveSocketClient(new ReactiveSocketClient() {
            @Override
            public Publisher<Payload> requestSubscription(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestStream(Payload payload) {
                return null;
            }

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
        Assert.assertTrue(0.5 == availability);
        client.pending = 1;
        availability = client.availability();
        Assert.assertTrue(1.0 > availability);
    }

    @Test
    public void testGoodRequest() {
        AtomicInteger integer = new AtomicInteger(2500);
        LoadEstimatorReactiveSocketClient client = new LoadEstimatorReactiveSocketClient(new ReactiveSocketClient() {
            @Override
            public Publisher<Payload> requestSubscription(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestStream(Payload payload) {
                return null;
            }

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
        LoadEstimatorReactiveSocketClient client = new LoadEstimatorReactiveSocketClient(new ReactiveSocketClient() {
            @Override
            public Publisher<Payload> requestSubscription(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestStream(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestResponse(Payload payload) {
                return s -> {
                    s.onSubscribe(EmptySubscription.INSTANCE);
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
        }, TimeUnit.SECONDS.toNanos(1), TimeUnit.SECONDS.toNanos(5));

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
            Assert.assertTrue(old > n);
            old = n;
        }
    }

}