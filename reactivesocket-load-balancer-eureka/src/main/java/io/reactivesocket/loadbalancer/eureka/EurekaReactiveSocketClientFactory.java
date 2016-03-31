package io.reactivesocket.loadbalancer.eureka;

import com.netflix.discovery.DiscoveryClient;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.XORShiftRandom;
import io.reactivesocket.loadbalancer.client.DelegatingReactiveSocket;
import io.reactivesocket.loadbalancer.client.FailureAwareDelegatingReactiveSocket;
import io.reactivesocket.loadbalancer.client.InitializingDelegatingReactiveSocket;
import io.reactivesocket.loadbalancer.client.LoadBalancerDelegatingReactiveSocket;
import io.reactivesocket.loadbalancer.client.LoadEstimatorDelegatingReactiveSocket;
import io.reactivesocket.loadbalancer.servo.ServoMetricsDelegatingReactiveSocket;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link ReactiveSocketFactory} that creates {@link DelegatingReactiveSocket}s that use a
 * {@link EurekaReactiveSocketClientFactory} to look instances to talk to.
 */
public class EurekaReactiveSocketClientFactory implements ReactiveSocketFactory<EurekaReactiveSocketClientFactory.EurekaReactiveSocketClientFactoryConfig, ReactiveSocket> {
    
    private DiscoveryClient discoveryClient;

    public EurekaReactiveSocketClientFactory(DiscoveryClient discoveryClient) {
        this.discoveryClient = discoveryClient;
    }

    @Override
    public Publisher<ReactiveSocket> call(EurekaReactiveSocketClientFactoryConfig config) {
        return s -> {
            EurekaSocketAddressFactory addressFactory = new EurekaSocketAddressFactory(
                discoveryClient,
                config.vip,
                config.secure,
                config.poolsize);

            LoadBalancerDelegatingReactiveSocket loadBalancerReactiveSocketClient = new LoadBalancerDelegatingReactiveSocket(
                addressFactory,
                addressFactory.getClosedConnectionProvider(),
                socketAddress -> {
                    InitializingDelegatingReactiveSocket initializingReactiveSocketClient
                        = new InitializingDelegatingReactiveSocket(
                        config.reactiveSocketFactory,
                        socketAddress,
                        config.connectionFailureTimeout,
                        config.connectionFailureTimeoutTimeUnit,
                        config.connectionFailureRetryWindow,
                        config.retryWindowUnit);

                    FailureAwareDelegatingReactiveSocket failureAwareReactiveSocketClient
                        = new FailureAwareDelegatingReactiveSocket(initializingReactiveSocketClient, config.failureWindow, config.failureWindowUnit);

                    return subscriber -> {
                        subscriber.onSubscribe(EmptySubscription.INSTANCE);
                        subscriber.onNext(new LoadEstimatorDelegatingReactiveSocket(failureAwareReactiveSocketClient, config.tauUp, config.tauDown));
                        subscriber.onComplete();
                    };
                },
                () -> XORShiftRandom.getInstance().randomInt());

            ServoMetricsDelegatingReactiveSocket servoMetricsDelegatingReactiveSocket = new ServoMetricsDelegatingReactiveSocket(loadBalancerReactiveSocketClient, config.vip);

            s.onSubscribe(EmptySubscription.INSTANCE);
            s.onNext(servoMetricsDelegatingReactiveSocket);
            s.onComplete();
        };
    }

    public static class EurekaReactiveSocketClientFactoryConfig {
        boolean secure;
        int poolsize;
        String vip;
        ReactiveSocketFactory reactiveSocketFactory;
        long connectionFailureTimeout;
        TimeUnit connectionFailureTimeoutTimeUnit;
        long connectionFailureRetryWindow;
        TimeUnit retryWindowUnit;
        double tauUp;
        double tauDown;
        long failureWindow;
        TimeUnit failureWindowUnit;

        public EurekaReactiveSocketClientFactoryConfig(String vip,
                                                       boolean secure,
                                                       int poolsize,
                                                       ReactiveSocketFactory reactiveSocketFactory,
                                                       long connectionFailureTimeout, 
                                                       TimeUnit connectionFailureTimeoutTimeUnit, 
                                                       long connectionFailureRetryWindow, 
                                                       TimeUnit retryWindowUnit, 
                                                       double tauUp, 
                                                       double tauDown, 
                                                       long failureWindow, 
                                                       TimeUnit failureWindowUnit) {
            this.secure = secure;
            this.vip = vip;
            this.poolsize = poolsize;
            this.reactiveSocketFactory = reactiveSocketFactory;
            this.connectionFailureTimeout = connectionFailureTimeout;
            this.connectionFailureTimeoutTimeUnit = connectionFailureTimeoutTimeUnit;
            this.connectionFailureRetryWindow = connectionFailureRetryWindow;
            this.retryWindowUnit = retryWindowUnit;
            this.tauUp = tauUp;
            this.tauDown = tauDown;
            this.failureWindow = failureWindow;
            this.failureWindowUnit = failureWindowUnit;
        }

        public static EurekaReactiveSocketClientFactoryConfig newInstance(
            String vip,
            int poolsize,
            ReactiveSocketFactory reactiveSocketFactory) {
            return new EurekaReactiveSocketClientFactoryConfig(
                vip,
                false,
                poolsize,
                reactiveSocketFactory,
                5,
                TimeUnit.SECONDS,
                2,
                TimeUnit.SECONDS,
                TimeUnit.SECONDS.toNanos(1),
                TimeUnit.SECONDS.toNanos(5),
                30,
                TimeUnit.SECONDS);
        }

        public boolean isSecure() {
            return secure;
        }

        public int getPoolsize() {
            return poolsize;
        }

        public String getVip() {
            return vip;
        }

        public ReactiveSocketFactory getReactiveSocketFactory() {
            return reactiveSocketFactory;
        }

        public long getConnectionFailureTimeout() {
            return connectionFailureTimeout;
        }

        public TimeUnit getConnectionFailureTimeoutTimeUnit() {
            return connectionFailureTimeoutTimeUnit;
        }

        public long getConnectionFailureRetryWindow() {
            return connectionFailureRetryWindow;
        }

        public TimeUnit getRetryWindowUnit() {
            return retryWindowUnit;
        }

        public double getTauUp() {
            return tauUp;
        }

        public double getTauDown() {
            return tauDown;
        }

        public long getFailureWindow() {
            return failureWindow;
        }

        public TimeUnit getFailureWindowUnit() {
            return failureWindowUnit;
        }


        public EurekaReactiveSocketClientFactoryConfig secure(boolean secure) {
            this.secure = secure;
            return this;
        }

        public EurekaReactiveSocketClientFactoryConfig poolsize(int poolsize) {
            this.poolsize = poolsize;
            return this;
        }

        public EurekaReactiveSocketClientFactoryConfig vip(String vip) {
            this.vip = vip;
            return this;
        }

        public EurekaReactiveSocketClientFactoryConfig reactiveSocketFactory(ReactiveSocketFactory reactiveSocketFactory) {
            this.reactiveSocketFactory = reactiveSocketFactory;
            return this;
        }

        public EurekaReactiveSocketClientFactoryConfig connectionFailureTimeout(long connectionFailureTimeout) {
            this.connectionFailureTimeout = connectionFailureTimeout;
            return this;
        }

        public EurekaReactiveSocketClientFactoryConfig connectionFailureTimeoutTimeUnit(TimeUnit connectionFailureTimeoutTimeUnit) {
            this.connectionFailureTimeoutTimeUnit = connectionFailureTimeoutTimeUnit;
            return this;
        }

        public EurekaReactiveSocketClientFactoryConfig connectionFailureRetryWindow(long connectionFailureRetryWindow) {
            this.connectionFailureRetryWindow = connectionFailureRetryWindow;
            return this;
        }

        public EurekaReactiveSocketClientFactoryConfig retryWindowUnit(TimeUnit retryWindowUnit) {
            this.retryWindowUnit = retryWindowUnit;
            return this;
        }

        public EurekaReactiveSocketClientFactoryConfig tauUp(double tauUp) {
            this.tauUp = tauUp;
            return this;
        }

        public EurekaReactiveSocketClientFactoryConfig tauDown(double tauDown) {
            this.tauDown = tauDown;
            return this;
        }

        public EurekaReactiveSocketClientFactoryConfig failureWindow(long failureWindow) {
            this.failureWindow = failureWindow;
            return this;
        }

        public EurekaReactiveSocketClientFactoryConfig failureWindowUnit(TimeUnit failureWindowUnit) {
            this.failureWindowUnit = failureWindowUnit;
            return this;
        }
    }
    
}
