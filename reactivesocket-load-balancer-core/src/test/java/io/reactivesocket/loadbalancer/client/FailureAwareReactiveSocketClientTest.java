package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by rroeser on 3/8/16.
 */
public class FailureAwareReactiveSocketClientTest {
    @Test
    public void testError() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        FailureAwareReactiveSocketClient client = new FailureAwareReactiveSocketClient(new ReactiveSocketClient() {

            @Override
            public Publisher<Void> metadataPush(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Void> fireAndForget(Payload payload) {
                return null;
            }

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
        }, 1, TimeUnit.SECONDS);

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
    public void testWidowReset() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        FailureAwareReactiveSocketClient client = new FailureAwareReactiveSocketClient(new ReactiveSocketClient() {
            @Override
            public Publisher<Void> metadataPush(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Void> fireAndForget(Payload payload) {
                return null;
            }

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
        }, 1, TimeUnit.SECONDS);

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

        Thread.sleep(1_001);

        double reset = client.availability();
        Assert.assertTrue(reset > bad);
    }
}