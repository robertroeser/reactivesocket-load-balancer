package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Calculates a clients availibility uses EWMA
 */
public class EMWAReactiveSocketClient implements ReactiveSocketClient {
    private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;

    private final long epoch = System.nanoTime();
    private final double tauUp;
    private final double tauDown;
    private final ReactiveSocketClient child;

    private volatile long stamp = epoch;  // last timestamp in nanos we observed an rtt
    volatile int pending = 0;     // instantaneous rate
    private volatile double cost = 0.0;   // ewma of rtt, sensitive to peaks.

    public EMWAReactiveSocketClient(ReactiveSocketClient child,
                                    double tauUp,
                                    double tauDown) {
        this.child = child;
        this.tauUp = tauUp;
        this.tauDown = tauDown;
    }

    @Override
    public double availability() {
        double childAvailability = child.availability();

        return (1 / getWeight()) * childAvailability;
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {

        return s ->
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
    }

    private double getWeight() {
        double weight;
        observe(0.0);
        if (cost == 0.0 && pending != 0) {
            weight = STARTUP_PENALTY + pending;
        } else {
            weight = cost * (pending+1);
        }

        return weight == 0.0 ? 1.0 : weight;
    }


    private void observe(double rtt) {
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
