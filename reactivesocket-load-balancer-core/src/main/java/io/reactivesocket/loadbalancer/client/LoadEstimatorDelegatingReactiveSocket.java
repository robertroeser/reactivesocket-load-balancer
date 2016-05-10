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
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Calculates a clients availibility uses EWMA
 */
public class LoadEstimatorDelegatingReactiveSocket implements DelegatingReactiveSocket {
    private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;

    private final long epoch = System.nanoTime();
    private final double tauUp;
    private final double tauDown;
    private final ReactiveSocket child;

    private volatile long stamp = epoch;  // last timestamp in nanos we observed an rtt
    volatile int pending = 0;     // instantaneous rate
    private volatile double cost = 0.0;   // ewma of rtt, sensitive to peaks.

    private AtomicLong count;

    public LoadEstimatorDelegatingReactiveSocket(ReactiveSocket child,
                                                 double tauUp,
                                                 double tauDown) {
        this.child = child;
        this.tauUp = tauUp;
        this.tauDown = tauDown;
        this.count = new AtomicLong();
    }

    @Override
    public double availability() {
        double childAvailability = child.availability();
        double currentCount = count.get();
        double availability = (1.0 / getWeight() * 0.5) + (1.0 / currentCount == 0 ? 1.0 : currentCount * 0.5);
        return availability * childAvailability;
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.requestResponse(payload).subscribe(new Subscriber<Payload>() {
                final long start = System.nanoTime();

                @Override
                public void onSubscribe(Subscription s) {
                    pending += 1;
                    s.request(1);
                }

                @Override
                public void onNext(Payload payload) {
                    s.onNext(payload);
                }

                @Override
                public void onError(Throwable t) {
                    pending -= 1;
                    observe(System.nanoTime() - start);
                    s.onError(t);
                }

                @Override
                public void onComplete() {
                    pending -= 1;
                    observe(System.nanoTime() - start);
                    s.onComplete();
                }
            });
        };
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.requestStream(payload)
                .subscribe(new Subscriber<Payload>() {
                    Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        s.request(1);
                        count.incrementAndGet();
                    }

                    @Override
                    public void onNext(Payload payload) {
                        s.onNext(payload);
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        count.decrementAndGet();
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        count.decrementAndGet();
                    }
                });
        };
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.requestSubscription(payload)
                .subscribe(new Subscriber<Payload>() {
                    Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        s.request(1);
                        count.incrementAndGet();
                    }

                    @Override
                    public void onNext(Payload payload) {
                        s.onNext(payload);
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        count.decrementAndGet();
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        count.decrementAndGet();
                    }
                });
        };
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.fireAndForget(payload)
                .subscribe(new Subscriber<Void>() {
                    Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        s.request(1);
                        count.incrementAndGet();
                    }

                    @Override
                    public void onNext(Void payload) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        count.decrementAndGet();
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        count.decrementAndGet();
                    }
                });
        };
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            child.metadataPush(payload)
                .subscribe(new Subscriber<Void>() {
                    Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        s.request(1);
                        count.incrementAndGet();
                    }

                    @Override
                    public void onNext(Void payload) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        count.decrementAndGet();
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        count.decrementAndGet();
                    }
                });
        };
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        throw new UnsupportedOperationException();
    }


    private synchronized double getWeight() {
        double weight;
        observe(0.0);
        if (cost == 0.0 && pending != 0) {
            weight = STARTUP_PENALTY + pending;
        } else {
            weight = cost * (pending+1);
        }

        return weight == 0.0 ? 1.0 : weight;
    }

    private synchronized void observe(double rtt) {
        long t = System.nanoTime();
        long td = Math.max(t - stamp, 0L);
        double tau;
        // different convergence speed (i.e. go up faster that you go down)
        if (rtt > cost) {
            tau = tauUp;
        } else {
            tau = tauDown;
        }
        double w = Math.exp(-td / tau);
        cost = cost * w + rtt * (1.0 - w);
        stamp = t;
    }

    @Override
    public void close() throws Exception {
        child.close();
    }

}
