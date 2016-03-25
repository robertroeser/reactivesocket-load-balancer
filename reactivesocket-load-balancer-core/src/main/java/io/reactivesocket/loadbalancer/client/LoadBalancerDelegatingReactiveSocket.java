package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.ClosedConnectionsProvider;
import io.reactivesocket.loadbalancer.SocketAddressFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of {@link DelegatingReactiveSocket} will load balance across a pool of childern reactive sockets clients.
 * It uses power of two choice to select the client. This client is "always" available.
 */
public class LoadBalancerDelegatingReactiveSocket implements DelegatingReactiveSocket {
    private final SocketAddressFactory socketAddressFactory;

    private final ClosedConnectionsProvider closedConnectionsProvider;

    private final Map<SocketAddress, DelegatingReactiveSocket> clientMap;

    private final ReactiveSocketFactory<SocketAddress, DelegatingReactiveSocket> reactiveSocketClientFactory;

    private final NumberGenerator numberGenerator;

    private static final NoAvailableReactiveSocketClientsException NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION = new NoAvailableReactiveSocketClientsException();

    public LoadBalancerDelegatingReactiveSocket(SocketAddressFactory socketAddressFactory,
                                                ClosedConnectionsProvider closedConnectionsProvider,
                                                ReactiveSocketFactory<SocketAddress, DelegatingReactiveSocket> reactiveSocketClientFactory,
                                                NumberGenerator numberGenerator) {
        this.socketAddressFactory = socketAddressFactory;
        this.closedConnectionsProvider = closedConnectionsProvider;
        this.clientMap = new ConcurrentHashMap<>();
        this.reactiveSocketClientFactory = reactiveSocketClientFactory;
        this.numberGenerator = numberGenerator;
    }

    @Override
    public double availability() {
        return 1;
    }

    <T> Publisher<T> loadBalance(Action action, Payload payload) {
        Publisher<T> payloadPublisher = s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            socketAddressFactory
                .call()
                .subscribe(new Subscriber<List<SocketAddress>>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(List<SocketAddress> socketAddresses) {
                        final int size = socketAddresses.size();

                        // No address return an exception
                        if (size == 0) {
                            onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                        } else if (size == 1) {
                            DelegatingReactiveSocket delegatingReactiveSocket = getReactiveSocketClient(socketAddresses.get(0));
                            // If the one connection isn't available return an exception
                            if (delegatingReactiveSocket.availability() == 0) {
                                onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                            } else {
                                action.call(s, delegatingReactiveSocket, payload);
                            }
                        } else if (size == 2) {
                            SocketAddress socketAddress1 = socketAddresses.get(0);
                            SocketAddress socketAddress2 = socketAddresses.get(1);

                            DelegatingReactiveSocket rsc1 = getReactiveSocketClient(socketAddress1);
                            DelegatingReactiveSocket rsc2 = getReactiveSocketClient(socketAddress2);

                            if (rsc1.availability() == 0 && rsc2.availability() == 0) {
                                s.onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                            } else if (rsc1.availability() > rsc2.availability()) {
                                action.call(s, rsc1, payload);
                            } else {
                                action.call(s, rsc2, payload);
                            }
                        } else {
                            DelegatingReactiveSocket rsc1 = null;
                            DelegatingReactiveSocket rsc2 = null;
                            for (int i = 0; i < 5; i++) {
                                if (rsc1 == null) rsc1 = getRandomReactiveSocketClient(socketAddresses, size);
                                if (rsc2 == null) rsc2 = getRandomReactiveSocketClient(socketAddresses, size);
                                if (rsc1 == rsc2) rsc2 = null;
                                else if (rsc1 != null && rsc2 != null) break;
                            }

                            if (rsc1 != null && rsc2 != null) {
                                if (rsc1.availability() == 0 && rsc2.availability() == 0) {
                                    onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                                } else if (rsc1.availability() > rsc2.availability()) {
                                    action.call(s, rsc1, payload);
                                } else {
                                    action.call(s, rsc2, payload);
                                }
                            } else if (rsc1 == null && rsc2.availability() > 0) {
                                action.call(s, rsc2, payload);
                            } else if (rsc2 == null && rsc1.availability() > 0) {
                                action.call(s, rsc1, payload);
                            } else if (rsc1 == null && rsc2.availability() == 0) {
                                onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                            } else if (rsc2 == null && rsc1.availability() == 0) {
                                onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                            } else {
                                onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        closedConnectionsProvider.call().subscribe(new Subscriber<List<SocketAddress>>() {
                            @Override
                            public void onSubscribe(Subscription s) {
                                s.request(Long.MAX_VALUE);
                            }

                            @Override
                            public void onNext(List<SocketAddress> socketAddresses) {
                                socketAddresses.forEach(socketAddress -> {
                                    try {
                                        ReactiveSocket removed = clientMap.remove(socketAddress);
                                        if (removed != null) {
                                            removed.close();
                                        }
                                    } catch (Throwable t) {
                                        t.printStackTrace();
                                    }
                                });
                            }

                            @Override
                            public void onError(Throwable t) {

                            }

                            @Override
                            public void onComplete() {

                            }
                        });
                    }
                });
        };

        return payloadPublisher;
    }


    @FunctionalInterface
    interface Action<T> {
        void call(Subscriber <? super T> subscriber, DelegatingReactiveSocket client, Payload payload);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return loadBalance(
            (s, r, p) ->
                r
                    .requestResponse(p)
                    .subscribe(new Subscriber<Payload>() {
                        Subscription subscription;
                        @Override
                        public void onSubscribe(Subscription s) {
                            s.request(1);
                            subscription = s;
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
                    })
            , payload);

        //return loadBalance(this::delegateRequestResponse, payload);
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return loadBalance(
            (s, r, p) ->
                r
                    .requestSubscription(p)
                    .subscribe(new Subscriber<Payload>() {
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
                        }

                        @Override
                        public void onComplete() {
                            s.onComplete();
                        }
                    })
            , payload);
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return loadBalance(
            (s, r, p) ->
                r
                    .requestStream(p)
                    .subscribe(new Subscriber<Payload>() {
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
                        }

                        @Override
                        public void onComplete() {
                            s.onComplete();
                        }
                    })
            , payload);
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return loadBalance(
            (s, r, p) ->
                r
                    .fireAndForget(p)
                    .subscribe(new Subscriber<Void>() {
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
                        }

                        @Override
                        public void onComplete() {
                            s.onComplete();
                        }
                    })
            , payload);
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return loadBalance(
            (s, r, p) ->
                r
                    .metadataPush(p)
                    .subscribe(new Subscriber<Void>() {
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
                        }

                        @Override
                        public void onComplete() {
                            s.onComplete();
                        }
                    })
            , payload);
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return null;
    }

    DelegatingReactiveSocket getRandomReactiveSocketClient(List<SocketAddress> socketAddresses, int size) {
        SocketAddress socketAddress = socketAddresses.get(numberGenerator.nextInt() % size);
        DelegatingReactiveSocket delegatingReactiveSocket = getReactiveSocketClient(socketAddress);

        return delegatingReactiveSocket;
    }

    DelegatingReactiveSocket getReactiveSocketClient(SocketAddress socketAddress) {
        return clientMap.computeIfAbsent(socketAddress, reactiveSocketClientFactory::callAndWait);
    }

    @FunctionalInterface
    public interface NumberGenerator {
        default int nextInt() {
            return Math.abs(generateInt());
        }

        int generateInt();
    }

    @Override
    public void close() throws Exception {
        clientMap.values().forEach(client -> {
            try {
                client.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        });
    }
}
