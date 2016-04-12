package io.reactivesocket.loadbalancer.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

public class DynamicTrafficSplittingReactiveSocket implements ReactiveSocket {
    private final ReactiveSocket a;
    private final ReactiveSocket b;
    private final IntSupplier permyriadSupplier;
    private final IntSupplier numberGenerator;

    private volatile ReactiveSocket next;

    /**
     * @param a
     * @param b
     * @param permyriadSupplier retrieves the permyriad of traffic to be sent to ReactiveSocket b
     * @param numberGenerator generates a number between 0 to 10,000
     */
    public DynamicTrafficSplittingReactiveSocket(ReactiveSocket a, ReactiveSocket b, IntSupplier permyriadSupplier, IntSupplier numberGenerator) {
        this.a = a;
        this.b = b;
        this.permyriadSupplier = permyriadSupplier;
        this.numberGenerator = numberGenerator;
    }

    public DynamicTrafficSplittingReactiveSocket(ReactiveSocket a, ReactiveSocket b, IntSupplier permyriadSupplier) {
        this(a, b, permyriadSupplier, () -> ThreadLocalRandom.current().nextInt(0, 10_000));
    }

    synchronized void selectNextReactiveSocket() {
        if (numberGenerator.getAsInt() <= permyriadSupplier.getAsInt()) {
            next = b;
        } else {
            next = a;
        }
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        try {
            return next.requestResponse(payload);
        } finally {
            selectNextReactiveSocket();
        }
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        try {
            return next.fireAndForget(payload);
        } finally {
            selectNextReactiveSocket();
        }
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        try {
            return next.requestStream(payload);
        } finally {
            selectNextReactiveSocket();
        }
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        try {
            return next.requestSubscription(payload);
        } finally {
            selectNextReactiveSocket();
        }
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        try {
            return next.requestChannel(payloads);
        } finally {
            selectNextReactiveSocket();
        }
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        try {
            return next.metadataPush(payload);
        } finally {
            selectNextReactiveSocket();
        }
    }

    @Override
    public double availability() {
        return next.availability();
    }

    @Override
    public void start(Completable c) {
        c.success();
    }

    @Override
    public void onRequestReady(Consumer<Throwable> c) {

    }

    @Override
    public void onRequestReady(Completable c) {
        c.success();
    }

    @Override
    public void sendLease(int ttl, int numberOfRequests) {
        a.sendLease(ttl, numberOfRequests);
        b.sendLease(ttl, numberOfRequests);
    }

    @Override
    public void shutdown() {
        a.shutdown();
        b.shutdown();
    }

    @Override
    public void close() throws Exception {
        a.close();
        b.close();
    }
}
