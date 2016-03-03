package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ReactiveSocketClient that keeps track of successes and failures and uses them to compute availability.
 */
public class FailureAwareReactiveSocketClient implements ReactiveSocketClient {
    private final ReactiveSocketClient child;
    private final long window;
    private final AtomicLong success;
    private final AtomicLong failure;
    private volatile long lastWindowTs;

    public FailureAwareReactiveSocketClient(ReactiveSocketClient child, long window, TimeUnit unit) {
        this.child = child;
        this.window = unit.toNanos(window);
        this.success = new AtomicLong();
        this.failure = new AtomicLong();

        this.lastWindowTs = System.nanoTime();
    }

    @Override
    public double availability() {
        double childAvailability = child.availability();

        // If the window is expired set success and failure to zero and return the child availability
        if ((System.nanoTime() - lastWindowTs) > window) {
            success.set(0);
            failure.set(0);

            return childAvailability;
        } else {
            long success = this.success.get();
            long failure = this.failure.get();

            // return 0 if only failures
            if (success == 0 && failure > 0) {
                return 0;
            }
            // return the childAvailability if success and failure are both zero
            else if (success == 0 && failure == 0) {
                return childAvailability;
            }
            else {
                return childAvailability * (success / (success + failure));
            }
        }
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return s ->
            child.requestResponse(payload).subscribe(new Subscriber<Payload>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(Payload payload) {
                    success.incrementAndGet();
                    s.onNext(payload);
                }

                @Override
                public void onError(Throwable t) {
                    failure.incrementAndGet();
                    s.onError(t);
                }

                @Override
                public void onComplete() {
                    s.onComplete();
                }
            });
    }

    @Override
    public void close() throws Exception {
        child.close();
    }
}