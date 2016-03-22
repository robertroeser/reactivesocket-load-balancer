package io.reactivesocket.loadbalancer.aeron;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.aeron.server.ReactiveSocketAeronServer;
import io.reactivesocket.exceptions.SetupException;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.loadbalancer.ClosedConnectionsProvider;
import io.reactivesocket.loadbalancer.SocketAddressFactory;
import io.reactivesocket.loadbalancer.client.FailureAwareReactiveSocketClient;
import io.reactivesocket.loadbalancer.client.InitializingReactiveSocketClient;
import io.reactivesocket.loadbalancer.client.LoadBalancerReactiveSocketClient;
import io.reactivesocket.loadbalancer.client.LoadEstimatorReactiveSocketClient;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.BitUtil;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Created by rroeser on 3/21/16.
 */
@Ignore
public class AeronReactiveSocketFactoryTest {

    @Test
    public void testCall() {
        MediaDriver.Context ctx = new MediaDriver.Context();
        ctx.dirsDeleteOnStart(true);
        MediaDriver mediaDriver = MediaDriver.launch(ctx);

        ReactiveSocketAeronServer
            .create("localhost", 39790, new ConnectionSetupHandler() {
                @Override
                public RequestHandler apply(ConnectionSetupPayload setupPayload) throws SetupException {
                    RequestHandler.Builder builder = new RequestHandler.Builder();
                    builder.withRequestResponse(new Function<Payload, Publisher<Payload>>() {
                        @Override
                        public Publisher<Payload> apply(Payload payload) {
                            return new Publisher<Payload>() {
                                @Override
                                public void subscribe(Subscriber<? super Payload> s) {
                                    s.onSubscribe(EmptySubscription.INSTANCE);
                                    s.onNext(new Payload() {
                                        @Override
                                        public ByteBuffer getData() {
                                            ByteBuffer byteBuffer = ByteBuffer.allocate(BitUtil.SIZE_OF_INT);
                                            byteBuffer.putInt(2);
                                            return byteBuffer;
                                        }

                                        @Override
                                        public ByteBuffer getMetadata() {
                                            return null;
                                        }
                                    });
                                }
                            };
                        }
                    });

                    return builder.build();
                }
            });

        InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 39790);
        AeronReactiveSocketFactory aeronReactiveSocketFactory = new AeronReactiveSocketFactory("localhost", 39790, ConnectionSetupPayload.create("int", "int"), t -> t.printStackTrace());
        Publisher<ReactiveSocket> call = aeronReactiveSocketFactory.call(address, 2, TimeUnit.SECONDS);
        Observable<ReactiveSocket> reactiveSocketObservable = RxReactiveStreams.toObservable(call);
        TestSubscriber testSubscriber = new TestSubscriber();
        reactiveSocketObservable.doOnError(t -> t.printStackTrace()).subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();
    }

    @Test
    public void testRequestResponse() {
        MediaDriver.Context ctx = new MediaDriver.Context();
        ctx.dirsDeleteOnStart(true);
        MediaDriver mediaDriver = MediaDriver.launch(ctx);

        ReactiveSocketAeronServer
            .create("localhost", 39790, new ConnectionSetupHandler() {
                @Override
                public RequestHandler apply(ConnectionSetupPayload setupPayload) throws SetupException {
                    RequestHandler.Builder builder = new RequestHandler.Builder();
                    builder.withRequestResponse(new Function<Payload, Publisher<Payload>>() {
                        @Override
                        public Publisher<Payload> apply(Payload payload) {
                            return new Publisher<Payload>() {
                                @Override
                                public void subscribe(Subscriber<? super Payload> s) {
                                    s.onSubscribe(EmptySubscription.INSTANCE);
                                    s.onNext(new Payload() {
                                        @Override
                                        public ByteBuffer getData() {
                                            System.out.println("server sending 2");
                                            ByteBuffer byteBuffer = ByteBuffer.allocate(BitUtil.SIZE_OF_INT);
                                            byteBuffer.putInt(2);
                                            return byteBuffer;
                                        }

                                        @Override
                                        public ByteBuffer getMetadata() {
                                            return null;
                                        }
                                    });
                                }
                            };
                        }
                    });

                    return builder.build();
                }
            });

        AeronReactiveSocketFactory aeronReactiveSocketFactory = new AeronReactiveSocketFactory("localhost", 39790, ConnectionSetupPayload.create("int", "int"), Throwable::printStackTrace);

        LoadBalancerReactiveSocketClient loadBalancerReactiveSocketClient
            = new LoadBalancerReactiveSocketClient(
            new SocketAddressFactory() {
                @Override
                public Publisher<List<SocketAddress>> call() {
                    return s -> {
                        InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 39790);
                        List<SocketAddress> lists = new ArrayList<>();
                        lists.add(address);
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(lists);
                        s.onComplete();
                    };
                }
            },
            new ClosedConnectionsProvider() {
                @Override
                public Publisher<List<SocketAddress>> call() {
                    return s -> {
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onNext(new ArrayList<SocketAddress>());
                        s.onComplete();
                    };
                }
            },
            socketAddress -> {
                InitializingReactiveSocketClient initializingReactiveSocketClient
                    = new InitializingReactiveSocketClient(
                    aeronReactiveSocketFactory,
                    socketAddress,
                    5,
                    TimeUnit.SECONDS,
                    2,
                    TimeUnit.SECONDS);

                FailureAwareReactiveSocketClient failureAwareReactiveSocketClient
                    = new FailureAwareReactiveSocketClient(initializingReactiveSocketClient,
                    30,
                    TimeUnit.SECONDS);

                LoadEstimatorReactiveSocketClient loadEstimatorReactiveSocketClient = new LoadEstimatorReactiveSocketClient(failureAwareReactiveSocketClient, 2, 5);

                return loadEstimatorReactiveSocketClient;

            },
            () -> ThreadLocalRandom.current().nextInt()
        );

        Publisher<Payload> payloadPublisher = loadBalancerReactiveSocketClient.requestResponse(new Payload() {
            @Override
            public ByteBuffer getData() {
                System.out.println("sending 1");
                ByteBuffer allocate = ByteBuffer.allocate(BitUtil.SIZE_OF_INT);
                allocate.putInt(1);
                return allocate;
            }

            @Override
            public ByteBuffer getMetadata() {
                return null;
            }
        });

        Observable<Payload> payloadObservable =
            RxReactiveStreams.toObservable(payloadPublisher);

        TestSubscriber testSubscriber = new TestSubscriber();
        payloadObservable.doOnError(t -> t.printStackTrace()).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();
    }
}