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
public class FailureAwareDelegatingReactiveSocketTest {
    @Test
    public void testError() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        FailureAwareDelegatingReactiveSocket client = new FailureAwareDelegatingReactiveSocket(new DelegatingReactiveSocket() {

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
            public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
                return null;
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

        TestSubscriber<Payload> subscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertCompleted();
        double good = client.availability();

        subscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertError(RuntimeException.class);
        double bad = client.availability();
        Assert.assertTrue(good > bad);
    }

    @Test
    public void testWidowReset() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        FailureAwareDelegatingReactiveSocket client = new FailureAwareDelegatingReactiveSocket(new DelegatingReactiveSocket() {
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
            public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
                return null;
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

        TestSubscriber<Payload> subscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertCompleted();
        double good = client.availability();

        subscriber = new TestSubscriber<>();
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