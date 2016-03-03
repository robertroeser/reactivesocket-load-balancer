package io.reactivesocket.loadbalancer;

import io.reactivesocket.loadbalancer.client.ReactiveSocketClient;

import java.util.function.Function;

/**
 * Factory that creates {@link ReactiveSocketClient}
 * @param <T>
 */
public interface ReactiveSocketClientFactory<T> extends Function<T,ReactiveSocketClient> {
    @Override
    ReactiveSocketClient apply(T t);
}
