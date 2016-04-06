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
public class LoadEstimatorDelegatingReactiveSocketTest {
    @Test
    public void testStartupPenalty() {
        LoadEstimatorDelegatingReactiveSocket client = new LoadEstimatorDelegatingReactiveSocket(new DelegatingReactiveSocket() {
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
            public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
                return null;
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
        LoadEstimatorDelegatingReactiveSocket client = new LoadEstimatorDelegatingReactiveSocket(new DelegatingReactiveSocket() {
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
            public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
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

        TestSubscriber<Payload> subscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertCompleted();

        double old = client.availability();

        for (int i = 0; i < 0; i++) {
            subscriber = new TestSubscriber<>();
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
        LoadEstimatorDelegatingReactiveSocket client = new LoadEstimatorDelegatingReactiveSocket(new DelegatingReactiveSocket() {
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
            public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
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

        TestSubscriber<Payload> subscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertCompleted();

        double old = client.availability();

        for (int i = 0; i < 5; i++) {
            subscriber = new TestSubscriber<>();
            RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
            subscriber.awaitTerminalEvent();
            subscriber.assertCompleted();
            double n = client.availability();
            Assert.assertTrue(old > n);
            old = n;
        }
    }

}