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
package io.reactivesocket.loadbalancer.servo;

import io.reactivesocket.Payload;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.client.DelegatingReactiveSocket;
import io.reactivesocket.loadbalancer.servo.internal.HdrHistogramServoTimer;
import io.reactivesocket.loadbalancer.servo.internal.ThreadLocalAdderCounter;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * An implementation of {@link DelegatingReactiveSocket} that sends metrics to Servo
 */
public class ServoMetricsDelegatingReactiveSocket implements DelegatingReactiveSocket {
    private final DelegatingReactiveSocket child;

    private final String prefix;

    final ThreadLocalAdderCounter success;

    final ThreadLocalAdderCounter failure;

    final HdrHistogramServoTimer timer;

    public ServoMetricsDelegatingReactiveSocket(DelegatingReactiveSocket child, String prefix) {
        this.child = child;
        this.prefix = prefix;

        this.success = ThreadLocalAdderCounter.newThreadLocalAdderCounter(prefix + "_success");
        this.failure = ThreadLocalAdderCounter.newThreadLocalAdderCounter(prefix + "_failure");
        this.timer = HdrHistogramServoTimer.newInstance(prefix + "_timer");
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        long start = recordStart();
        return (Subscriber<? super Payload> s) ->
            delegateRequestResponse(s,
                child,
                payload,
                () -> recordSuccess(start),
                () -> recordFailure(start));
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        long start = recordStart();
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.requestStream(payload).subscribe(new Subscriber<Payload>() {
                Subscription subscription;

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                    subscription = s;
                }

                @Override
                public void onNext(Payload payload) {
                    s.onNext(payload);
                    subscription.request(1);
                }

                @Override
                public void onError(Throwable t) {
                    s.onError(t);
                    recordFailure(start);
                }

                @Override
                public void onComplete() {
                    s.onComplete();
                    recordSuccess(start);
                }
            });
        };
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        long start = recordStart();
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.requestSubscription(payload).subscribe(new Subscriber<Payload>() {
                Subscription subscription;

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                    subscription = s;
                }

                @Override
                public void onNext(Payload payload) {
                    s.onNext(payload);
                    subscription.request(1);
                }

                @Override
                public void onError(Throwable t) {
                    s.onError(t);
                    recordFailure(start);
                }

                @Override
                public void onComplete() {
                    s.onComplete();
                    recordSuccess(start);
                }
            });
        };
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        long start = recordStart();
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.fireAndForget(payload).subscribe(new Subscriber<Void>() {
                Subscription subscription;

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                    subscription = s;
                }

                @Override
                public void onNext(Void payload) {
                }

                @Override
                public void onError(Throwable t) {
                    s.onError(t);
                    recordFailure(start);
                }

                @Override
                public void onComplete() {
                    s.onComplete();
                    recordSuccess(start);
                }
            });
        };
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        long start = recordStart();
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.metadataPush(payload).subscribe(new Subscriber<Void>() {
                Subscription subscription;

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                    subscription = s;
                }

                @Override
                public void onNext(Void payload) {
                }

                @Override
                public void onError(Throwable t) {
                    s.onError(t);
                    recordFailure(start);
                }

                @Override
                public void onComplete() {
                    s.onComplete();
                    recordSuccess(start);
                }
            });
        };
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        throw new UnsupportedOperationException();
    }

    private long recordStart() {
        return System.nanoTime();
    }

    private void recordFailure(long start) {
        failure.increment();
        timer.record(System.nanoTime() - start);
    }

    private void recordSuccess(long start) {
        success.increment();
        timer.record(System.nanoTime() - start);
    }

    @Override
    public void close() throws Exception {
        child.close();
    }
}
