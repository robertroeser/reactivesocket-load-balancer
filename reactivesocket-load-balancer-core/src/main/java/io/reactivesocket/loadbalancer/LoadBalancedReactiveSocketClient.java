package io.reactivesocket.loadbalancer;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LoadBalancedReactiveSocketClient implements AutoCloseable {
    private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;
    private final ReactiveSocketFactory reactiveSocketFactory;

    private final double tauUp;
    private final double tauDown;
    private final double exceptionPenalty;
    private final double connectionFailurePenalty;
    private final long epoch = System.nanoTime();
    private final SocketAddress socketAddress;

    private volatile long stamp = epoch;  // last timestamp in nanos we observed an rtt
    private volatile int pending = 0;     // instantaneous rate
    private volatile double cost = 0.0;   // ewma of rtt, sensitive to peaks.
    private volatile ReactiveSocket reactiveSocket;
    private volatile Throwable reactiveSocketConnectionException;

    LoadBalancedReactiveSocketClient(
        ReactiveSocketFactory reactiveSocketFactory,
        SocketAddress socketAddress,
        double tauUp,
        double tauDown,
        double connectionFailurePenalty,
        double exceptionPenalty) {
        this.tauUp = tauUp;
        this.tauDown = tauDown;
        this.reactiveSocketFactory = reactiveSocketFactory;
        this.exceptionPenalty = exceptionPenalty;
        this.connectionFailurePenalty = connectionFailurePenalty;
        this.socketAddress = socketAddress;
    }

    @Override
    public void close() throws Exception {
        reactiveSocket.close();
    }

    void checkReactiveSocket() throws Exception {
        if (reactiveSocket == null) {
            synchronized (this) {
                if (reactiveSocket == null) {
                    CountDownLatch latch = new CountDownLatch(1);
                    reactiveSocketFactory
                        .call(socketAddress, 5, TimeUnit.SECONDS)
                        .subscribe(new Subscriber<ReactiveSocket>() {
                            @Override
                            public void onSubscribe(Subscription s) {
                                s.request(1);
                            }

                            @Override
                            public void onNext(ReactiveSocket r) {
                                reactiveSocket = r;
                            }

                            @Override
                            public void onError(Throwable t) {
                                reactiveSocketConnectionException = t;
                                latch.countDown();
                            }

                            @Override
                            public void onComplete() {
                                latch.countDown();
                            }
                        });

                    latch.await();

                    if (reactiveSocketConnectionException != null) {
                        throw new Exception(reactiveSocketConnectionException);
                    }
                }
            }
        }
    }

    public Publisher<Payload> requestResponse(Payload payload) {
        return (Subscriber<? super Payload> s) -> {
            final long start = System.nanoTime();
            try {
                checkReactiveSocket();
            } catch (Throwable t) {
                s.onError(t);
                observe((System.nanoTime() - start) * connectionFailurePenalty);
                return;
            }

            pending += 1;

            Publisher<Payload> payloadPublisher = reactiveSocket
                .requestResponse(payload);

            payloadPublisher
                .subscribe(new Subscriber<Payload>() {
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
                        pending -= 1;
                        observe((System.nanoTime() - start) * exceptionPenalty);
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

    public Publisher<Void> fireAndForget(Payload payload) {
        throw new IllegalStateException();
    }

    public Publisher<Payload> requestStream(Payload payload) {
        throw new IllegalStateException();
    }

    public Publisher<Payload> requestSubscription(Payload payload) {
        throw new IllegalStateException();
    }

    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        throw new IllegalStateException();
    }

    public Publisher<Void> metadataPush(Payload payload) {
        throw new IllegalStateException();
    }

    double getWeight() {
        observe(0.0);
        if (cost == 0.0 && pending != 0) {
            return STARTUP_PENALTY + pending;
        } else {
            return cost * (pending+1);
        }
    }

    private void observe(double rtt) {
        long t = System.nanoTime();
        long td = Math.max(t - stamp, 0L);
        if (rtt > cost) {
            double w = Math.exp(-td / tauUp);
            cost = cost * w + rtt * (1.0 - w);
        } else {
            double w = Math.exp(-td / tauDown);
            cost = cost * w + rtt * (1.0 - w);
        }
        stamp = t;
    }
}
