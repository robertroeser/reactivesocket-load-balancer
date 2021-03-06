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
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;

/**
 * ReactiveSocketClient that keeps track of successes and failures and uses them to compute availability.
 */
public class FailureAwareDelegatingReactiveSocket implements DelegatingReactiveSocket {
    private final ReactiveSocket child;
    private final long epoch = System.nanoTime();
    private final double tau;

    private long stamp = epoch;
    private volatile double ewmaErrorPercentage = 1.0; // 1.0 = 100% success, 0.0 = 0% successes

    public FailureAwareDelegatingReactiveSocket(ReactiveSocket child, long window, TimeUnit unit) {
        this.child = child;
        this.tau = unit.toNanos(window);
    }

    @Override
    public double availability() {
        double childAvailability = child.availability();

        // If the window is expired set success and failure to zero and return the child availability
        if ((System.nanoTime() - stamp) > tau) {
            updateErrorPercentage(1.0);
        }

        return childAvailability * ewmaErrorPercentage;
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.requestResponse(payload).subscribe(new Subscriber<Payload>() {
                Subscription subscription;
                @Override
                public void onSubscribe(Subscription s) {
                    subscription = s;
                    s.request(1);
                }

                @Override
                public void onNext(Payload payload) {
                    updateErrorPercentage(1.0);
                    s.onNext(payload);
                }

                @Override
                public void onError(Throwable t) {
                    updateErrorPercentage(0.0);
                    s.onError(t);
                }

                @Override
                public void onComplete() {
                    s.onComplete();
                }
            });
        };
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.requestSubscription(payload).subscribe(new Subscriber<Payload>() {
                Subscription subscription;
                @Override
                public void onSubscribe(Subscription s) {
                    subscription = s;
                    s.request(1);
                }

                @Override
                public void onNext(Payload payload) {
                    updateErrorPercentage(1.0);
                    s.onNext(payload);
                    subscription.request(1);
                }

                @Override
                public void onError(Throwable t) {
                    updateErrorPercentage(0.0);
                    s.onError(t);
                }

                @Override
                public void onComplete() {
                    s.onComplete();
                }
            });
        };
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.requestStream(payload).subscribe(new Subscriber<Payload>() {
                Subscription subscription;
                @Override
                public void onSubscribe(Subscription s) {
                    subscription = s;
                    s.request(1);
                }

                @Override
                public void onNext(Payload payload) {
                    updateErrorPercentage(1.0);
                    s.onNext(payload);
                    subscription.request(1);
                }

                @Override
                public void onError(Throwable t) {
                    updateErrorPercentage(0.0);
                    s.onError(t);
                }

                @Override
                public void onComplete() {
                    s.onComplete();
                }
            });
        };
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.fireAndForget(payload).subscribe(new Subscriber<Void>() {
                Subscription subscription;
                @Override
                public void onSubscribe(Subscription s) {
                    subscription = s;
                    s.request(1);
                }

                @Override
                public void onNext(Void payload) {
                }

                @Override
                public void onError(Throwable t) {
                    updateErrorPercentage(0.0);
                    s.onError(t);
                }

                @Override
                public void onComplete() {
                    updateErrorPercentage(1.0);
                    s.onComplete();
                }
            });
        };
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.metadataPush(payload).subscribe(new Subscriber<Void>() {
                Subscription subscription;
                @Override
                public void onSubscribe(Subscription s) {
                    subscription = s;
                    s.request(1);
                }

                @Override
                public void onNext(Void payload) {
                }

                @Override
                public void onError(Throwable t) {
                    updateErrorPercentage(0.0);
                    s.onError(t);
                }

                @Override
                public void onComplete() {
                    updateErrorPercentage(1.0);
                    s.onComplete();
                }
            });
        };
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * @param value 1.0 for success, 0.0 for a failure
     */
    private void updateErrorPercentage(double value) {
        long t = System.nanoTime();
        long td = Math.max(t - stamp, 0L);
        double w = Math.exp(-td / tau);
        synchronized(this) {
            ewmaErrorPercentage = ewmaErrorPercentage * w + value * (1.0 - w);
        }
        stamp = t;
    }

    @Override
    public void close() throws Exception {
        child.close();
    }
}