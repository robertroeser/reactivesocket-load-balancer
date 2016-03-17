package io.reactivesocket.loadbalancer.eureka;

import com.netflix.discovery.DiscoveryClient;
import io.reactivesocket.loadbalancer.ReactiveSocketClientFactory;
import io.reactivesocket.loadbalancer.ReactiveSocketFactory;
import io.reactivesocket.loadbalancer.XORShiftRandom;
import io.reactivesocket.loadbalancer.client.LoadEstimatorReactiveSocketClient;
import io.reactivesocket.loadbalancer.client.FailureAwareReactiveSocketClient;
import io.reactivesocket.loadbalancer.client.InitializingReactiveSocketClient;
import io.reactivesocket.loadbalancer.client.LoadBalancerReactiveSocketClient;
import io.reactivesocket.loadbalancer.client.ReactiveSocketClient;
import io.reactivesocket.loadbalancer.servo.ServoMetricsReactiveSocketClient;

import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link ReactiveSocketClientFactory} that creates {@link ReactiveSocketClient}s that use a
 * {@link EurekaReactiveSocketClientFactory} to look instances to talk to.
 */
public class EurekaReactiveSocketClientFactory implements ReactiveSocketClientFactory<EurekaReactiveSocketClientFactory.EurekaReactiveSocketClientFactoryConfig> {
    
    private DiscoveryClient discoveryClient;

    public EurekaReactiveSocketClientFactory(DiscoveryClient discoveryClient) {
        this.discoveryClient = discoveryClient;
    }
    
    @Override
    public ReactiveSocketClient apply(EurekaReactiveSocketClientFactoryConfig config) {
        EurekaSocketAddressFactory addressFactory = new EurekaSocketAddressFactory(
            discoveryClient,
            config.vip,
            config.secure,
            config.poolsize);

        LoadBalancerReactiveSocketClient loadBalancerReactiveSocketClient = new LoadBalancerReactiveSocketClient(
            addressFactory,
            addressFactory.getClosedConnectionProvider(),
            socketAddress -> {
                InitializingReactiveSocketClient initializingReactiveSocketClient
                    = new InitializingReactiveSocketClient(
                    config.reactiveSocketFactory,
                    socketAddress,
                    config.connectionFailureTimeout,
                    config.connectionFailureTimeoutTimeUnit,
                    config.connectionFailureRetryWindow,
                    config.retryWindowUnit);

                FailureAwareReactiveSocketClient failureAwareReactiveSocketClient
                    = new FailureAwareReactiveSocketClient(initializingReactiveSocketClient, config.failureWindow, config.failureWindowUnit);

                LoadEstimatorReactiveSocketClient loadEstimatorReactiveSocketClient = new LoadEstimatorReactiveSocketClient(failureAwareReactiveSocketClient, config.tauUp, config.tauDown);

                return loadEstimatorReactiveSocketClient;
            },
            () -> XORShiftRandom.getInstance().randomInt());


        ServoMetricsReactiveSocketClient servoMetricsReactiveSocketClient = new ServoMetricsReactiveSocketClient(loadBalancerReactiveSocketClient, config.vip);

        return servoMetricsReactiveSocketClient;
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
