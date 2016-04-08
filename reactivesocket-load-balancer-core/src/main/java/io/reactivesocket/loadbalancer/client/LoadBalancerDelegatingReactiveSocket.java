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
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.internal.rx.EmptySubscription;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * An implementation of {@link ReactiveSocket} will load balance across a pool of childern reactive sockets clients.
 * It uses power of two choice to select the client. This client is "always" available.
 */
public class LoadBalancerDelegatingReactiveSocket<T> implements DelegatingReactiveSocket {
    private final Supplier<Publisher<List<T>>> connectionsProvider;

    private final Supplier<Publisher<List<T>>> closedConnectionsProvider;

    private final Map<T, ReactiveSocket> clientMap;

    private final ReactiveSocketFactory<T, ? extends ReactiveSocket> reactiveSocketClientFactory;

    private final NumberGenerator numberGenerator;

    private static final NoAvailableReactiveSocketClientsException NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION =
        new NoAvailableReactiveSocketClientsException();

    private static final double MAX_PROBABILITY_PICKING_UNAVAILABLE = .01; // p
    private static final double PERCENTAGE_OF_UNAVAILABLE_SERVERS = .25;   // b

    // `Effort` is computed so that we have a probability `p` of NOT finding a good server
    // even though there's only `b%` of unavailable servers.
    // Here, if we want 1% probability of not finding a suitable server when 25% of the cluster is
    // unavailable, then `effort = 6`
    //
    // (The formula is derived from simple probability)
    private static final int EFFORT = (int) Math.ceil(
        Math.log(MAX_PROBABILITY_PICKING_UNAVAILABLE)
            / Math.log((PERCENTAGE_OF_UNAVAILABLE_SERVERS * (2 - PERCENTAGE_OF_UNAVAILABLE_SERVERS )))
    );

    public LoadBalancerDelegatingReactiveSocket(Supplier<Publisher<List<T>>> connectionsProvider,
                                                Supplier<Publisher<List<T>>> closedConnectionsProvider,
                                                ReactiveSocketFactory<T, ? extends ReactiveSocket> reactiveSocketClientFactory,
                                                NumberGenerator numberGenerator) {
        this.connectionsProvider = connectionsProvider;
        this.closedConnectionsProvider = closedConnectionsProvider;
        this.clientMap = new ConcurrentHashMap<>();
        this.reactiveSocketClientFactory = reactiveSocketClientFactory;
        this.numberGenerator = numberGenerator;
    }

    @Override
    public double availability() {
        return 1;
    }

    <X> Publisher<X> loadBalance(Action<X> action, Payload payload) {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            connectionsProvider.get()
                .subscribe(new Subscriber<List<T>>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(List<T> connections) {
                        final int size = connections.size();

                        // No address return an exception
                        if (size == 0) {
                            onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                        } else if (size == 1) {
                            ReactiveSocket reactiveSocket = getReactiveSocketClient(connections.get(0));
                            // If the one connection isn't available return an exception
                            if (reactiveSocket.availability() == 0.0) {
                                onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                            } else {
                                action.call(s, reactiveSocket, payload);
                            }
                        } else {
                            ReactiveSocket rsc1 = null;
                            ReactiveSocket rsc2 = null;
                            for (int i = 0; i < EFFORT; i++) {
                                int i1 = numberGenerator.nextInt() % size;
                                int i2 = numberGenerator.nextInt() % (size - 1);
                                if (i2 >= i1) {
                                    i2 = (i2 + 1) % size;
                                }
                                rsc1 = getRandomReactiveSocketClient(connections, i1);
                                rsc2 = getRandomReactiveSocketClient(connections, i2);
                                if (rsc1.availability() > 0.0 && rsc2.availability() > 0.0)
                                    break;
                            }

                            if (rsc1.availability() == 0.0 && rsc2.availability() == 0.0) {
                                onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                            } else if (rsc1.availability() > rsc2.availability()) {
                                action.call(s, rsc1, payload);
                            } else {
                                action.call(s, rsc2, payload);
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        closedConnectionsProvider.get().subscribe(new Subscriber<List<T>>() {
                            @Override
                            public void onSubscribe(Subscription s) {
                                s.request(Long.MAX_VALUE);
                            }

                            @Override
                            public void onNext(List<T> connections) {
                                connections.forEach(connection -> {
                                    try {
                                        ReactiveSocket removed = clientMap.remove(connection);
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
    }


    @FunctionalInterface
    interface Action<T> {
        void call(Subscriber <? super T> subscriber, ReactiveSocket client, Payload payload);
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

    private ReactiveSocket getRandomReactiveSocketClient(List<T> connections, int index) {
        T connection = connections.get(index);
        return getReactiveSocketClient(connection);
    }

    private ReactiveSocket getReactiveSocketClient(T connection) {
        return clientMap.computeIfAbsent(connection, reactiveSocketClientFactory::callAndWait);
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
