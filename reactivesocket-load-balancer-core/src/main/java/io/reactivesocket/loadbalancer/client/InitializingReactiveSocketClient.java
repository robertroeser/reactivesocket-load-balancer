package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.ReactiveSocketFactory;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class InitializingReactiveSocketClient implements ReactiveSocketClient {
    private final ReactiveSocketFactory reactiveSocketFactory;
    private final SocketAddress socketAddress;

    private final long connectionFailureRetryWindow;
    private final TimeUnit retryWindowUnit;

    private final long timeout;
    private final TimeUnit unit;

    //Visible for testing
    final Semaphore guard;
    final List<Completable> awaitingReactiveSocket;
    
    private volatile  ReactiveSocket reactiveSocket;
    
    private volatile long connectionFailureTimestamp = 0;


    public InitializingReactiveSocketClient(
        ReactiveSocketFactory reactiveSocketFactory, 
        SocketAddress socketAddress, 
        long timeout, 
        TimeUnit timeoutTimeUnit,
        long connectionFailureRetryWindow,
        TimeUnit retryWindowUnit) {
        this.reactiveSocketFactory = reactiveSocketFactory;
        this.socketAddress = socketAddress;
        this.timeout = timeout;
        this.unit = timeoutTimeUnit;
        this.guard = new Semaphore(1);
        this.awaitingReactiveSocket = new CopyOnWriteArrayList<>();
        this.connectionFailureRetryWindow = connectionFailureRetryWindow;
        this.retryWindowUnit = retryWindowUnit;
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

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        if (reactiveSocket == null) {
            if (guard.tryAcquire()) {
                Publisher<ReactiveSocket> reactiveSocketPublisher
                    = reactiveSocketFactory.call(socketAddress, timeout, unit);

                return s -> {
                    s.onSubscribe(EmptySubscription.INSTANCE);
                    reactiveSocketPublisher
                        .subscribe(new Subscriber<ReactiveSocket>() {
                            @Override
                            public void onSubscribe(Subscription s) {
                                s.request(1);
                            }

                            @Override
                            public void onNext(ReactiveSocket rSocket) {
                                reactiveSocket = rSocket;
                                reactiveSocket
                                    .requestResponse(payload)
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
                                            s.onError(t);
                                        }

                                        @Override
                                        public void onComplete() {
                                            s.onComplete();
                                        }
                                    });
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
                Publisher<Payload> payloadPublisher = s1 -> {
                    s1.onSubscribe(EmptySubscription.INSTANCE);
                    Completable completable = new Completable() {
                        @Override
                        public void success() {
                            reactiveSocket
                                .requestResponse(payload)
                                .subscribe(new Subscriber<Payload>() {
                                    @Override
                                    public void onSubscribe(Subscription s) {
                                        s.request(1);
                                    }

                                    @Override
                                    public void onNext(Payload payload) {
                                        s1.onNext(payload);
                                    }

                                    @Override
                                    public void onError(Throwable t) {
                                        s1.onError(t);
                                    }

                                    @Override
                                    public void onComplete() {
                                        s1.onComplete();
                                    }
                                });
                        }

                        @Override
                        public void error(Throwable e) {
                            s1.onError(e);
                        }
                    };
                    
                    awaitingReactiveSocket.add(completable);
                };
                
                return payloadPublisher;
            }
        } else {
            return reactiveSocket
                .requestResponse(payload);
        }
    }

    @Override
    public void close() throws Exception {
        if (reactiveSocket != null) {
            reactiveSocket.close();
        }
    }
}
