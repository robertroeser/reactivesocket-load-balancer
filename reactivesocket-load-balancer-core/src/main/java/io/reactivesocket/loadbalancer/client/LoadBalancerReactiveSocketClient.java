package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.ClosedConnectionsProvider;
import io.reactivesocket.loadbalancer.ReactiveSocketClientFactory;
import io.reactivesocket.loadbalancer.SocketAddressFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of {@link ReactiveSocketClient} will load balance across a pool of childern reactive sockets clients.
 * It uses power of two choice to select the client. This client is "always" available.
 */
public class LoadBalancerReactiveSocketClient implements ReactiveSocketClient {
    private final SocketAddressFactory socketAddressFactory;

    private final ClosedConnectionsProvider closedConnectionsProvider;

    private final Map<SocketAddress, ReactiveSocketClient> clientMap;

    private final ReactiveSocketClientFactory<SocketAddress> reactiveSocketClientFactory;

    private final NumberGenerator numberGenerator;

    private static final NoAvailableReactiveSocketClientsException NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION = new NoAvailableReactiveSocketClientsException();

    public LoadBalancerReactiveSocketClient(SocketAddressFactory socketAddressFactory,
                                            ClosedConnectionsProvider closedConnectionsProvider,
                                            ReactiveSocketClientFactory<SocketAddress> reactiveSocketClientFactory,
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

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        Publisher<Payload> payloadPublisher = s -> {
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
                            ReactiveSocketClient reactiveSocketClient = getReactiveSocketClient(socketAddresses.get(0));
                            // If the one connection isn't available return an exception
                            if (reactiveSocketClient.availability() == 0) {
                                onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                            } else {
                                delegateRequestResponse(s, reactiveSocketClient, payload);
                            }
                        } else if (size == 2) {
                            SocketAddress socketAddress1 = socketAddresses.get(0);
                            SocketAddress socketAddress2 = socketAddresses.get(1);

                            ReactiveSocketClient rsc1 = getReactiveSocketClient(socketAddress1);
                            ReactiveSocketClient rsc2 = getReactiveSocketClient(socketAddress2);

                            if (rsc1.availability() == 0 && rsc2.availability() == 0) {
                                s.onError(NO_AVAILABLE_REACTIVE_SOCKET_CLIENTS_EXCEPTION);
                            } else if (rsc1.availability() > rsc2.availability()) {
                                delegateRequestResponse(s, rsc1, payload);
                            } else {
                                delegateRequestResponse(s, rsc2, payload);
                            }
                        } else {
                            ReactiveSocketClient rsc1 = null;
                            ReactiveSocketClient rsc2 = null;
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
                                    delegateRequestResponse(s, rsc1, payload);
                                } else {
                                    delegateRequestResponse(s, rsc2, payload);
                                }
                            } else if (rsc1 == null && rsc2.availability() > 0) {
                                delegateRequestResponse(s, rsc2, payload);
                            } else if (rsc2 == null && rsc1.availability() > 0) {
                                delegateRequestResponse(s, rsc1, payload);
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
                                        ReactiveSocketClient removed = clientMap.remove(socketAddress);
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

    ReactiveSocketClient getRandomReactiveSocketClient(List<SocketAddress> socketAddresses, int size) {
        SocketAddress socketAddress = socketAddresses.get(numberGenerator.nextInt() % size);
        ReactiveSocketClient reactiveSocketClient = getReactiveSocketClient(socketAddress);

        return reactiveSocketClient;
    }

    ReactiveSocketClient getReactiveSocketClient(SocketAddress socketAddress) {
        return clientMap.computeIfAbsent(socketAddress, reactiveSocketClientFactory::apply);
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
