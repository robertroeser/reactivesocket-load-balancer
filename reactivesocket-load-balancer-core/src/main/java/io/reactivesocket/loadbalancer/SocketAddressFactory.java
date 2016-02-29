package io.reactivesocket.loadbalancer;

import org.reactivestreams.Publisher;

import java.net.SocketAddress;
import java.util.List;

@FunctionalInterface
public interface SocketAddressFactory {
    Publisher<List<SocketAddress>> call();
}
