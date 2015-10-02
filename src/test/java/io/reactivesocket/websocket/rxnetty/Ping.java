package io.reactivesocket.websocket.rxnetty;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.websocket.rxnetty.client.WebSocketClientDuplexConnection;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.ws.WebSocketConnection;
import io.reactivex.netty.protocol.http.ws.client.WebSocketResponse;
import org.HdrHistogram.Recorder;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Ping {
    public static void main(String... args) throws Exception {

        Observable<WebSocketConnection> wsConnection = HttpClient.newClient("localhost", 8888)
            //.enableWireLogging(LogLevel.ERROR)
            .createGet("/rs")
            .requestWebSocketUpgrade()
            .flatMap(WebSocketResponse::getWebSocketConnection);

        Publisher<WebSocketDuplexConnection> connectionPublisher = WebSocketClientDuplexConnection.create(RxReactiveStreams.toPublisher(wsConnection));

        ReactiveSocket reactiveSocket = RxReactiveStreams
            .toObservable(connectionPublisher)
            .map(w -> ReactiveSocket.fromClientConnection(w, ConnectionSetupPayload.create("UTF-8", "UTF-8")))
            .toBlocking()
            .single();

        reactiveSocket.startAndWait();

        byte[] data = new byte[4];
        ThreadLocalRandom.current().nextBytes(data);

        Payload keyPayload = new Payload() {
            @Override
            public ByteBuffer getData() {
                return ByteBuffer.wrap(data);
            }

            @Override
            public ByteBuffer getMetadata() {
                return null;
            }
        };

        CountDownLatch latch = new CountDownLatch(Integer.MAX_VALUE);
        final Recorder histogram = new Recorder(3600000000000L, 3);

        Schedulers
            .computation()
            .createWorker()
            .schedulePeriodically(() -> {
                System.out.println("---- PING/ PONG HISTO ----");
                histogram.getIntervalHistogram().outputPercentileDistribution(System.out, 5, 1000.0, false);
                System.out.println("---- PING/ PONG HISTO ----");
            }, 10, 10, TimeUnit.SECONDS);

        Observable
            .range(1, Integer.MAX_VALUE)
            .flatMap(i -> {
                long start = System.nanoTime();

                return RxReactiveStreams
                    .toObservable(
                        reactiveSocket
                            .requestResponse(keyPayload))
                    .doOnNext(s -> {
                        long diff = System.nanoTime() - start;
                        histogram.recordValue(diff);
                    });
            })
            .subscribe(new Subscriber<Payload>() {
                @Override
                public void onCompleted() {

                }

                @Override
                public void onError(Throwable e) {
                    e.printStackTrace();
                }

                @Override
                public void onNext(Payload payload) {
                    latch.countDown();
                }
            });

        latch.await();
        System.out.println("Sent => " + Integer.MAX_VALUE);
        System.exit(0);
    }
}
