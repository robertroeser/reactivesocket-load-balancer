package io.reactivesocket.loadbalancer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ReactiveSocketLoadBalancer {

    private SocketAddressFactory socketAddressFactory;

    private ClosedConnectionsProvider closedConnections;

    private Function<SocketAddress, LoadBalancedReactiveSocketClient> reactiveSocketFactory;

    private NumberGenerator numberGenerator;

    Map<SocketAddress, LoadBalancedReactiveSocketClient> clientMap = new ConcurrentHashMap<>();

    public ReactiveSocketLoadBalancer(
        SocketAddressFactory socketAddressFactory,
        ClosedConnectionsProvider closedConnections,
        ReactiveSocketFactory reactiveSocketFactory) {
        this(
            socketAddressFactory,
            closedConnections,
            reactiveSocketFactory,
            NUMBER_GENERATOR);
    }

    public ReactiveSocketLoadBalancer(
        SocketAddressFactory socketAddressFactory,
        ClosedConnectionsProvider closedConnections,
        ReactiveSocketFactory reactiveSocketFactory,
        NumberGenerator numberGenerator) {
        this.socketAddressFactory = socketAddressFactory;
        this.reactiveSocketFactory = (SocketAddress socketAddress) ->
             new LoadBalancedReactiveSocketClient(
                reactiveSocketFactory,
                socketAddress,
                NANOSECONDS.convert(1, SECONDS),
                NANOSECONDS.convert(15, SECONDS),
                2,
                5);
        this.closedConnections = closedConnections;
        this.numberGenerator = numberGenerator;
    }

    public Publisher<LoadBalancedReactiveSocketClient> nextAvailableSocket() {
        Publisher<LoadBalancedReactiveSocketClient> clientPublisher = (Subscriber<? super LoadBalancedReactiveSocketClient> s) ->
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
                        if (size == 1) {
                            SocketAddress socketAddress = socketAddresses.get(0);
                            LoadBalancedReactiveSocketClient loadBalancedReactiveSocketClient = clientMap.get(socketAddress);
                            s.onNext(loadBalancedReactiveSocketClient);
                        } else {
                            int first = numberGenerator.nextInt() % size;
                            int second = numberGenerator.nextInt() % size;

                            SocketAddress firstSocket = socketAddresses.get(first);
                            SocketAddress secondSocket = socketAddresses.get(second);

                            LoadBalancedReactiveSocketClient client1 = clientMap.computeIfAbsent(firstSocket, reactiveSocketFactory::apply);
                            LoadBalancedReactiveSocketClient client2 = clientMap.computeIfAbsent(secondSocket, reactiveSocketFactory::apply);

                            double weight1 = client1.getWeight();
                            double weight2 = client2.getWeight();

                            if (weight1 < weight2) {
                                s.onNext(client1);
                            } else {
                                s.onNext(client2);
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        closedConnections
                            .call()
                            .subscribe(new Subscriber<List<SocketAddress>>() {
                                @Override
                                public void onSubscribe(Subscription s) {
                                    s.request(1);
                                }

                                @Override
                                public void onNext(List<SocketAddress> socketAddresses) {
                                    socketAddresses
                                        .forEach(socketAddress -> {
                                            LoadBalancedReactiveSocketClient remove = clientMap.remove(socketAddress);
                                            try {
                                                remove.close();
                                            } catch (Throwable t) {}
                                        });
                                }

                                @Override
                                public void onError(Throwable t) {

                                }

                                @Override
                                public void onComplete() {

                                }
                            });

                        s.onComplete();
                    }
                });

        return clientPublisher;
    }

    @FunctionalInterface
    public interface NumberGenerator {
        int nextInt();
    }

    /*
     * Simple Implementations
     */
    public static class StaticListFactory implements SocketAddressFactory {
        private List<SocketAddress> socketAddresses;

        private StaticListFactory(List<SocketAddress> socketAddresses) {
            this.socketAddresses = socketAddresses;
        }

        @Override
        public Publisher<List<SocketAddress>> call() {
            return (Subscriber<? super List<SocketAddress>> s) -> {
                s.onNext(socketAddresses);
                s.onComplete();
            };
        }

        public static StaticListFactory newInstance(SocketAddress socketAddress, SocketAddress... socketAddresses) {
            List<SocketAddress> addressList  = new ArrayList<>();
            addressList.add(socketAddress);
            addressList.addAll(Arrays.asList(socketAddresses));
            return new StaticListFactory(addressList);
        }
    }

    private static final List<SocketAddress> EMPTY_LIST = new ArrayList<>();
    public final static ClosedConnectionsProvider NO_CHANGE_PROVIDER = () -> (Subscriber<? super List<SocketAddress>> s) -> {
        s.onNext(EMPTY_LIST);
        s.onComplete();
    };

    public final static NumberGenerator NUMBER_GENERATOR = () -> Math.abs(XORShiftRandom.getInstance().randomInt());
}
