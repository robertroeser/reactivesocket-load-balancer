package io.reactivesocket.loadbalancer.servo;

import io.reactivesocket.Payload;
import io.reactivesocket.loadbalancer.client.ReactiveSocketClient;
import io.reactivesocket.loadbalancer.servo.internal.HdrHistogramServoTimer;
import io.reactivesocket.loadbalancer.servo.internal.ThreadLocalAdderCounter;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

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
