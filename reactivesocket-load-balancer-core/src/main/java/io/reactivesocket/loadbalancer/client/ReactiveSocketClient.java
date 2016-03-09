package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import io.reactivesocket.internal.rx.EmptySubscription;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public interface ReactiveSocketClient extends AutoCloseable {

    /**
     * Calculates the availability of the client from 1.0 to 0.0 where
     * 1.0 is the best and 0.0 is the worst
     *
     * @return an availability score between 1.0 and 0.0
     */
    default double availability() {
        return 1;
    }

    Publisher<Payload> requestResponse(Payload payload);

    /**
     * Convenient way to delegate your request/response to the another ReactiveSocketClient. You call this inside a
     * {@link Publisher}
     * @param subscriber the subscriber from the outer publisher
     * @param client the reactivesocket client to delegate too
     * @param payload the payload that is being sent
     */
    default void delegateRequestResponse(Subscriber<? super Payload> subscriber, ReactiveSocketClient client, Payload payload) {
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

    /**
     * Convenient way to delegate your request/response to the another ReactiveSocketClient. You call this inside a
     * {@link Publisher}
     * @param subscriber the subscriber from the outer publisher
     * @param client the reactivesocket client to delegate too
     * @param payload the payload that is being sent
     */
    default void delegateRequestResponse(Subscriber<? super Payload> subscriber, ReactiveSocketClient client, Payload payload, Runnable doOnComplete) {
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
    default void delegateRequestResponse(Subscriber<? super Payload> subscriber, ReactiveSocketClient client, Payload payload, Runnable doOnComplete, Runnable doOnError) {
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
