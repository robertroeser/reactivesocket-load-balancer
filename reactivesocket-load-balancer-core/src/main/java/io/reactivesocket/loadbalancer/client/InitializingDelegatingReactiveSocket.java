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
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class InitializingDelegatingReactiveSocket<T> implements DelegatingReactiveSocket {
    private final ReactiveSocketFactory<T, ? extends ReactiveSocket> reactiveSocketFactory;
    private final T configuration;

    private final long connectionFailureRetryWindow;
    private final TimeUnit retryWindowUnit;

    private final long timeout;
    private final TimeUnit unit;

    //Visible for testing
    final Semaphore guard;
    final List<Completable> awaitingReactiveSocket;

    private volatile ReactiveSocket reactiveSocket;

    private volatile long connectionFailureTimestamp = 0;

    private final ScheduledExecutorService scheduledExecutorService;

    public InitializingDelegatingReactiveSocket(
        ReactiveSocketFactory<T, ? extends ReactiveSocket> reactiveSocketFactory,
        T configuration,
        long timeout,
        TimeUnit timeoutTimeUnit,
        long connectionFailureRetryWindow,
        TimeUnit retryWindowUnit) {
        this(
            reactiveSocketFactory,
            configuration,
            timeout,
            timeoutTimeUnit,
            connectionFailureRetryWindow,
            retryWindowUnit,
            Executors.newSingleThreadScheduledExecutor(r -> new Thread("initializing-reactive-socket-timeout")));
    }

    public InitializingDelegatingReactiveSocket(
        ReactiveSocketFactory<T, ? extends ReactiveSocket> reactiveSocketFactory,
        T configuration,
        long timeout, 
        TimeUnit timeoutTimeUnit,
        long connectionFailureRetryWindow,
        TimeUnit retryWindowUnit,
        ScheduledExecutorService scheduledExecutorService) {
        this.reactiveSocketFactory = reactiveSocketFactory;
        this.configuration = configuration;
        this.timeout = timeout;
        this.unit = timeoutTimeUnit;
        this.guard = new Semaphore(1);
        this.awaitingReactiveSocket = new CopyOnWriteArrayList<>();
        this.connectionFailureRetryWindow = connectionFailureRetryWindow;
        this.retryWindowUnit = retryWindowUnit;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    public double availability() {
        double availability = 0.0;
        if (reactiveSocket != null) {
            availability = reactiveSocket.availability();
        } else {
            long elapsed = System.nanoTime() - connectionFailureTimestamp;
            if (elapsed > retryWindowUnit.toNanos(connectionFailureRetryWindow)) {
                availability = 1.0;
            }
        }
        
        return availability;
    }

    <X> Publisher<X> init(Action<X> action) {
        if (guard.tryAcquire()) {
            Publisher<? extends ReactiveSocket> reactiveSocketPublisher
                = reactiveSocketFactory.call(configuration, timeout, unit, scheduledExecutorService);

            return s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                reactiveSocketPublisher
                    .subscribe(new Subscriber<ReactiveSocket>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            s.request(1);
                        }

                        @Override
                        public void onNext(ReactiveSocket r) {
                            reactiveSocket = r;
                            action.call(s, r);
                        }

                        @Override
                        public void onError(Throwable t) {
                            connectionFailureTimestamp = System.nanoTime();
                            guard.release();
                            s.onError(t);
                            awaitingReactiveSocket.forEach(c -> c.error(t));
                        }

                        @Override
                        public void onComplete() {
                            guard.release();
                            awaitingReactiveSocket.forEach(Completable::success);
                        }
                    });
            };
        } else {
            return s -> {
                s.onSubscribe(EmptySubscription.INSTANCE);
                Completable completable = new Completable() {
                    @Override
                    public void success() {
                        action.call(s, reactiveSocket);
                    }

                    @Override
                    public void error(Throwable e) {
                        s.onError(e);
                    }
                };

                awaitingReactiveSocket.add(completable);
            };
        }
    }

    interface Action<T> {
        void call(Subscriber<? super T> subscriber, ReactiveSocket reactiveSocket);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        if (reactiveSocket == null) {
            return init((s, r) ->
                r.requestResponse(payload).subscribe(new Subscriber<Payload>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(Payload payload) {
                        s.onNext(payload);
                    }

                    @Override
                    public void onError(Throwable t) {
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        s.onComplete();
                    }
                }));
        } else {
            return reactiveSocket.requestResponse(payload);
        }
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        if (reactiveSocket == null) {
            return init((s, r) ->
                r.requestSubscription(payload).subscribe(new Subscriber<Payload>() {
                    Subscription subscription;
                    @Override
                    public void onSubscribe(Subscription s) {
                        this.subscription = s;
                        s.request(1);
                    }

                    @Override
                    public void onNext(Payload payload) {
                        s.onNext(payload);
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        s.onComplete();
                    }
                }));
        } else {
            return reactiveSocket.requestSubscription(payload);
        }
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        if (reactiveSocket == null) {
            return init((s, r) ->
                r.requestStream(payload).subscribe(new Subscriber<Payload>() {
                    Subscription subscription;
                    @Override
                    public void onSubscribe(Subscription s) {
                        this.subscription = s;
                        s.request(1);
                    }

                    @Override
                    public void onNext(Payload payload) {
                        s.onNext(payload);
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        s.onComplete();
                    }
                }));
        } else {
            return reactiveSocket.requestStream(payload);
        }
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        if (reactiveSocket == null) {
            return init((s, r) ->
                r.fireAndForget(payload).subscribe(new Subscriber<Void>() {
                    Subscription subscription;
                    @Override
                    public void onSubscribe(Subscription s) {
                        this.subscription = s;
                        s.request(1);
                    }

                    @Override
                    public void onNext(Void payload) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        s.onComplete();
                    }
                }));
        } else {
            return reactiveSocket.fireAndForget(payload);
        }
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        if (reactiveSocket == null) {
            return init((s, r) ->
                r.metadataPush(payload).subscribe(new Subscriber<Void>() {
                    Subscription subscription;
                    @Override
                    public void onSubscribe(Subscription s) {
                        this.subscription = s;
                        s.request(1);
                    }

                    @Override
                    public void onNext(Void payload) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        s.onComplete();
                    }
                }));
        } else {
            return reactiveSocket.metadataPush(payload);
        }
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        if (reactiveSocket != null) {
            reactiveSocket.close();
        }
    }
}
