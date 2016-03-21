package io.reactivesocket.loadbalancer.servo;

import io.reactivesocket.Payload;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.client.ReactiveSocketClient;
import io.reactivesocket.loadbalancer.servo.internal.HdrHistogramServoTimer;
import io.reactivesocket.loadbalancer.servo.internal.ThreadLocalAdderCounter;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * An implementation of {@link ReactiveSocketClient} that sends metrics to Servo
 */
public class ServoMetricsReactiveSocketClient implements ReactiveSocketClient {
    private final ReactiveSocketClient child;

    private final String prefix;

    final ThreadLocalAdderCounter success;

    final ThreadLocalAdderCounter failure;

    final HdrHistogramServoTimer timer;

    public ServoMetricsReactiveSocketClient(ReactiveSocketClient child, String prefix) {
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
