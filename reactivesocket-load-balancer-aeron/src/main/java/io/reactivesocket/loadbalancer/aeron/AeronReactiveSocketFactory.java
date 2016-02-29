package io.reactivesocket.loadbalancer.aeron;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.aeron.client.AeronClientDuplexConnection;
import io.reactivesocket.aeron.client.AeronClientDuplexConnectionFactory;
import io.reactivesocket.loadbalancer.ReactiveSocketFactory;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import uk.co.real_logic.agrona.LangUtil;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by rroeser on 2/28/16.
 */
public class AeronReactiveSocketFactory implements ReactiveSocketFactory {
    private final ConnectionSetupPayload connectionSetupPayload;
    private final Consumer<Throwable> errorStream;

    public AeronReactiveSocketFactory(ConnectionSetupPayload connectionSetupPayload, Consumer<Throwable> errorStream) {
        this(39790, connectionSetupPayload, errorStream);
    }

    public AeronReactiveSocketFactory(int port, ConnectionSetupPayload connectionSetupPayload, Consumer<Throwable> errorStream) {
        this.connectionSetupPayload = connectionSetupPayload;
        this.errorStream = errorStream;

        try {
            InetAddress iPv4InetAddress = getIPv4InetAddress();
            InetSocketAddress inetSocketAddress = new InetSocketAddress(iPv4InetAddress, port);

            AeronClientDuplexConnectionFactory.getInstance().addSocketAddressToHandleResponses(inetSocketAddress);
        } catch (Exception e) {
            LangUtil.rethrowUnchecked(e);
        }
    }

    @Override
    public Publisher<ReactiveSocket> call(SocketAddress address, long timeout, TimeUnit timeUnit) {
        Publisher<AeronClientDuplexConnection> aeronClientDuplexConnection
            = AeronClientDuplexConnectionFactory.getInstance().createAeronClientDuplexConnection(address);

        Publisher<ReactiveSocket> publisher = new Publisher<ReactiveSocket>() {
            @Override
            public void subscribe(Subscriber<? super ReactiveSocket> s) {
                aeronClientDuplexConnection
                    .subscribe(new Subscriber<AeronClientDuplexConnection>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            s.request(1);
                        }

                        @Override
                        public void onNext(AeronClientDuplexConnection connection) {
                            ReactiveSocket reactiveSocket = ReactiveSocket.fromClientConnection(connection, connectionSetupPayload, errorStream);
                            CountDownLatch latch = new CountDownLatch(1);
                            reactiveSocket.start(new Completable() {
                                @Override
                                public void success() {
                                    latch.countDown();
                                    s.onComplete();
                                }

                                @Override
                                public void error(Throwable e) {
                                    s.onError(e);
                                }
                            });

                            try {
                                latch.await(timeout, timeUnit);
                            } catch (InterruptedException e) {
                                s.onError(e);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            s.onError(t);
                        }

                        @Override
                        public void onComplete() {}
                    });
            }
        };

        return publisher;
    }

    private static InetAddress getIPv4InetAddress() throws SocketException, UnknownHostException {
        String os = System.getProperty("os.name").toLowerCase();

        if (os.contains("nix") || os.contains("nux")) {
            NetworkInterface ni = NetworkInterface.getByName("eth0");

            Enumeration<InetAddress> ias = ni.getInetAddresses();

            InetAddress iaddress;
            do {
                iaddress = ias.nextElement();
            } while (!(iaddress instanceof Inet4Address));

            return iaddress;
        }

        return InetAddress.getLocalHost();  // for Windows and OS X it should work well
    }
}
