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

import java.util.function.Consumer;

public interface DelegatingReactiveSocket extends ReactiveSocket {

    /**
     * Calculates the availability of the client from 1.0 to 0.0 where
     * 1.0 is the best and 0.0 is the worst
     *
     * @return an availability score between 1.0 and 0.0
     */
    default double availability() {
        return 1;
    }

    @Override
    default void start(Completable c) {
        c.success();
    }

    @Override
    default void onRequestReady(Consumer<Throwable> c) {
        c.accept(null);
    }

    @Override
    default void onRequestReady(Completable c) {
        c.success();
    }

    @Override
    default void sendLease(int ttl, int numberOfRequests) {
    }

    @Override
    default void shutdown() {
    }

    @Override
    default void close() throws Exception {
    }

    /**
     * Convenient way to delegate your request/response to the another ReactiveSocketClient. You call this inside a
     * {@link Publisher}
     * @param subscriber the subscriber from the outer publisher
     * @param client the reactivesocket client to delegate too
     * @param payload the payload that is being sent
     */
    default void delegateRequestResponse(Subscriber<? super Payload> subscriber, DelegatingReactiveSocket client, Payload payload) {
        subscriber.onSubscribe(EmptySubscription.INSTANCE);
        client
            .requestResponse(payload)
            .subscribe(new Subscriber<Payload>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(Payload payload) {
                    subscriber.onNext(payload);
                }

                @Override
                public void onError(Throwable t) {
                    subscriber.onError(t);
                }

                @Override
                public void onComplete() {
                    subscriber.onComplete();
                }
            });
    }

    default void delegateRequestSubscription(Subscriber<? super Payload> subscriber, DelegatingReactiveSocket client, Payload payload) {
        subscriber.onSubscribe(EmptySubscription.INSTANCE);
        client.requestSubscription(payload).subscribe(new Subscriber<Payload>() {
            Subscription subscription;
            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                s.request(1);
            }

            @Override
            public void onNext(Payload payload) {
                subscriber.onNext(payload);
                subscription.request(1);
            }

            @Override
            public void onError(Throwable t) {
                subscriber.onError(t);
            }

            @Override
            public void onComplete() {
                subscriber.onComplete();
            }
        });
    }

    /**
     * Convenient way to delegate your request/response to the another ReactiveSocketClient. You call this inside a
     * {@link Publisher}
     * @param subscriber the subscriber from the outer publisher
     * @param client the reactivesocket client to delegate too
     * @param payload the payload that is being sent
     */
    default void delegateRequestResponse(Subscriber<? super Payload> subscriber, DelegatingReactiveSocket client, Payload payload, Runnable doOnComplete) {
        subscriber.onSubscribe(EmptySubscription.INSTANCE);
        client
            .requestResponse(payload)
            .subscribe(new Subscriber<Payload>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(Payload payload) {
                    subscriber.onNext(payload);
                }

                @Override
                public void onError(Throwable t) {
                    subscriber.onError(t);
                }

                @Override
                public void onComplete() {
                    doOnComplete.run();
                    subscriber.onComplete();
                }
            });
    }

    /**
     * Convenient way to delegate your request/response to the another ReactiveSocketClient. You call this inside a
     * {@link Publisher}
     * @param subscriber the subscriber from the outer publisher
     * @param client the reactivesocket client to delegate too
     * @param payload the payload that is being sent
     */
    default void delegateRequestResponse(Subscriber<? super Payload> subscriber, DelegatingReactiveSocket client, Payload payload, Runnable doOnComplete, Runnable doOnError) {
        subscriber.onSubscribe(EmptySubscription.INSTANCE);
        client
            .requestResponse(payload)
            .subscribe(new Subscriber<Payload>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(Payload payload) {
                    subscriber.onNext(payload);
                }

                @Override
                public void onError(Throwable t) {
                    doOnError.run();
                    subscriber.onError(t);
                }

                @Override
                public void onComplete() {
                    doOnComplete.run();
                    subscriber.onComplete();
                }
            });
    }

}
