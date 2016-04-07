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
package io.reactivesocket.loadbalancer.eureka;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryClient;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.client.DelegatingReactiveSocket;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class EurekaSocketAddressFactory {
    private static final long TIMEOUT = TimeUnit.SECONDS.toNanos(15);

    private static final Logger logger = LoggerFactory.getLogger(EurekaSocketAddressFactory.class);

    private static final List<SocketAddress> EMPTY_LIST = new ArrayList<>();

    final private DiscoveryClient client;

    final Set<SocketAddress> pool;

    final List<SocketAddress> prunedList;

    final private int poolSize;

    final private String vip;

    final private boolean secure;

    final ReentrantLock reentrantLock;

    volatile long lastUpdate = 0;
    long p1,p2,p3,p4,p5,p6;

    public EurekaSocketAddressFactory(DiscoveryClient client, String vip, boolean secure, int poolSize) {
        this.client = client;
        this.poolSize = poolSize;
        this.vip = vip;
        this.secure = secure;
        this.pool = new CopyOnWriteArraySet<>();
        this.prunedList = new CopyOnWriteArrayList<>();
        this.reentrantLock = new ReentrantLock();
    }

    /**
     * An implementation of {@link Publisher} that returns a list of factories using Eureka
     */
    public Publisher<List<SocketAddress>> getConnectionProvider() {
        if (pool.isEmpty()) {
            synchronized (this) {
                if (pool.isEmpty()) {
                    List<InstanceInfo> instancesByVipAddress = client.getInstancesByVipAddress(vip, secure);
                    populateList(instancesByVipAddress);
                    lastUpdate = System.nanoTime();
                }
            }
        }

        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            try {
                s.onNext(new ArrayList<>(pool));
                s.onComplete();
            } finally {
                if (System.nanoTime() - lastUpdate > TIMEOUT) {
                    try {
                        if (reentrantLock.tryLock()) {
                            List<InstanceInfo> instancesByVipAddress = client.getInstancesByVipAddress(vip, secure);
                            pruneList(instancesByVipAddress);
                            populateList(instancesByVipAddress);
                            lastUpdate = System.nanoTime();
                        }
                    } finally {
                        if (reentrantLock.isHeldByCurrentThread()) {
                            reentrantLock.unlock();
                        }
                    }
                }
            }
        };
    }

    /**
     * Gets an implementation of the {@link Publisher}
     * that can be provided to the {@link DelegatingReactiveSocket} to clean up missing connections
     * @return an Observable of list connections that should be closed
     */
    public Publisher<List<SocketAddress>> getClosedConnectionProvider() {
        return s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            if (EMPTY_LIST.isEmpty()) {
                s.onNext(EMPTY_LIST);
            } else {
                List<SocketAddress> copy = new ArrayList<>(prunedList);
                prunedList.clear();
                s.onNext(copy);
            }
            s.onComplete();
        };
    }

    void pruneList(List<InstanceInfo> instancesByVipAddress) {
        List<InetSocketAddress> currentPrunedList = new ArrayList<>(pool.size());
        pool
            .forEach(socketAddress -> {
                InetSocketAddress address = (InetSocketAddress) socketAddress;

                boolean found = false;
                for (InstanceInfo instanceInfo : instancesByVipAddress) {
                    InetSocketAddress current = instanceInfoToSocketAddress(instanceInfo);
                    found = current.equals(address);

                    if (found) {
                        break;
                    }
                }

                if (!found) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Removing socket {}", address);
                    }

                    currentPrunedList.add(address);
                }

            });

        if (!currentPrunedList.isEmpty()) {
            prunedList.addAll(currentPrunedList);
            pool.removeAll(currentPrunedList);
        }
    }

    InetSocketAddress instanceInfoToSocketAddress(InstanceInfo instanceInfo) {
        return
            secure
                ? InetSocketAddress.createUnresolved(instanceInfo.getIPAddr(), instanceInfo.getSecurePort())
                : InetSocketAddress.createUnresolved(instanceInfo.getIPAddr(), instanceInfo.getPort());
    }

    void populateList(List<InstanceInfo> instancesByVipAddress) {
        Iterator<InstanceInfo> iter = instancesByVipAddress.iterator();
        while (pool.size() < poolSize && iter.hasNext()) {
            InetSocketAddress address = instanceInfoToSocketAddress(iter.next());
            if (pool.add(address)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Address {} not found in pool - adding", address);
                }
            }
        }
    }
}
