package io.reactivesocket.websocket.rxnetty.client;


import io.reactivesocket.websocket.rxnetty.WebSocketDuplexConnection;
import io.reactivex.netty.protocol.http.ws.WebSocketConnection;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class WebSocketClientDuplexConnection extends WebSocketDuplexConnection {

    public WebSocketClientDuplexConnection(WebSocketConnection webSocketConnection) {
        super(webSocketConnection, "client");
    }

    public static Publisher<WebSocketDuplexConnection> create(Publisher<WebSocketConnection> webSocketConnection) {
        Publisher<WebSocketDuplexConnection> duplexConnectionPublisher = new Publisher<WebSocketDuplexConnection>() {
            @Override
            public void subscribe(Subscriber<? super WebSocketDuplexConnection> child) {
                webSocketConnection
                    .subscribe(new Subscriber<WebSocketConnection>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            s.request(Long.MAX_VALUE);
                        }

                        @Override
                        public void onNext(WebSocketConnection webSocketConnection) {
                            WebSocketDuplexConnection connection = new WebSocketClientDuplexConnection(webSocketConnection);
                            child.onNext(connection);
                        }

                        @Override
                        public void onError(Throwable t) {
                            child.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            child.onComplete();
                        }
                    });
            }
        };

        return duplexConnectionPublisher;
    }
}
