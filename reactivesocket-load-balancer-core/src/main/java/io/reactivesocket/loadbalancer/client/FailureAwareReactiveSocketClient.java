package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import io.reactivesocket.internal.rx.EmptySubscription;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;

/**
 * ReactiveSocketClient that keeps track of successes and failures and uses them to compute availability.
 */
public class FailureAwareReactiveSocketClient implements ReactiveSocketClient {
    private final ReactiveSocketClient child;
    private final long epoch = System.nanoTime();
    private final double tau;

    private long stamp = epoch;
    private volatile double ewmaErrorPercentage = 1.0; // 1.0 = 100% success, 0.0 = 0% successes

    public FailureAwareReactiveSocketClient(ReactiveSocketClient child, long window, TimeUnit unit) {
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