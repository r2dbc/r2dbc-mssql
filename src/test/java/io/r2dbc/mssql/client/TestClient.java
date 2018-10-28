/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql.client;

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Test {@link Client} implementation.
 */
public final class TestClient implements Client {

    public static final TestClient NO_OP = new TestClient(false, Flux.empty(), TransactionStatus.AUTO_COMMIT);

    private final boolean expectClose;

    private final EmitterProcessor<Message> requestProcessor = EmitterProcessor.create(false);

    private final FluxSink<Message> requests = this.requestProcessor.sink();

    private final EmitterProcessor<Flux<Message>> responseProcessor = EmitterProcessor.create(false);

    private final TransactionStatus transactionStatus;

    private TestClient(boolean expectClose, Flux<Window> windows, TransactionStatus transactionStatus) {

        this.expectClose = expectClose;
        this.transactionStatus = transactionStatus;

        FluxSink<Flux<Message>> responses = this.responseProcessor.sink();

        Objects.requireNonNull(windows)
            .map(window -> window.exchanges)
            .map(exchanges -> exchanges
                .concatMap(exchange ->

                    this.requestProcessor.zipWith(exchange.requests)
                        .handle((tuple, sink) -> {
                            Message actual = tuple.getT1();
                            Message expected = tuple.getT2();

                            if (!actual.equals(expected)) {
                                sink.error(new AssertionError(String.format("Request %s was not the expected request %s", actual, expected)));
                            }
                        })
                        .thenMany(exchange.responses)))
            .subscribe(responses::next, responses::error, responses::complete);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Mono<Void> close() {
        return this.expectClose ? Mono.empty() : Mono.error(new AssertionError("close called unexpectedly"));
    }

    @Override
    public Flux<Message> exchange(Publisher<? extends ClientMessage> requests) {
        Objects.requireNonNull(requests, "requests must not be null");

        return this.responseProcessor
            .doOnSubscribe(s ->
                Flux.from(requests)
                    .subscribe(this.requests::next, this.requests::error))
            .next()
            .flatMapMany(Function.identity());
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return TestByteBufAllocator.TEST;
    }

    @Override
    public TransactionDescriptor getTransactionDescriptor() {
        return TransactionDescriptor.empty();
    }

    @Override
    public TransactionStatus getTransactionStatus() {
        return this.transactionStatus;
    }

    @Override
    public boolean isColumnEncryptionSupported() {
        return true;
    }

    public static final class Builder {

        private final List<Window.Builder<?>> windows = new ArrayList<>();

        private boolean expectClose = false;

        private TransactionStatus transactionStatus = TransactionStatus.AUTO_COMMIT;

        private Builder() {
        }

        public TestClient build() {
            return new TestClient(this.expectClose, Flux.fromIterable(this.windows).map(Window.Builder::build), transactionStatus);
        }

        public Builder expectClose() {
            this.expectClose = true;
            return this;
        }

        public Builder withTransactionStatus(TransactionStatus transactionStatus) {
            this.transactionStatus = Objects.requireNonNull(transactionStatus, "TransactionStatus must not be nul");
            return this;
        }

        public Exchange.Builder<Builder> expectRequest(ClientMessage... requests) {
            Objects.requireNonNull(requests);

            Window.Builder<Builder> window = new Window.Builder<>(this);
            this.windows.add(window);

            Exchange.Builder<Builder> exchange = new Exchange.Builder<>(this, requests);
            window.exchanges.add(exchange);

            return exchange;
        }


        public Window.Builder<Builder> window() {
            Window.Builder<Builder> window = new Window.Builder<>(this);
            this.windows.add(window);
            return window;
        }

    }

    private static final class Exchange {

        private final Flux<ClientMessage> requests;

        private final Publisher<Message> responses;

        private Exchange(Flux<ClientMessage> requests, Publisher<Message> responses) {
            this.requests = Objects.requireNonNull(requests);
            this.responses = Objects.requireNonNull(responses);
        }

        public static final class Builder<T> {

            private final T chain;

            private final Flux<ClientMessage> requests;

            private Publisher<Message> responses;

            private Builder(T chain, ClientMessage... requests) {
                this.chain = Objects.requireNonNull(chain);
                this.requests = Flux.just(Objects.requireNonNull(requests));
            }

            public T thenRespond(Message... responses) {
                Objects.requireNonNull(responses);

                return thenRespond(Flux.just(responses));
            }

            T thenRespond(Publisher<Message> responses) {
                Objects.requireNonNull(responses);

                this.responses = responses;
                return this.chain;
            }

            private Exchange build() {
                return new Exchange(this.requests, this.responses);
            }
        }
    }

    private static final class Window {

        private final Flux<Exchange> exchanges;

        private Window(Flux<Exchange> exchanges) {
            this.exchanges = Objects.requireNonNull(exchanges);
        }

        public static final class Builder<T> {

            private final T chain;

            private final List<Exchange.Builder<?>> exchanges = new ArrayList<>();

            private Builder(T chain) {
                this.chain = Objects.requireNonNull(chain);
            }

            public T done() {
                return this.chain;
            }

            public Exchange.Builder<Builder<T>> expectRequest(ClientMessage request) {
                Objects.requireNonNull(request);

                Exchange.Builder<Builder<T>> exchange = new Exchange.Builder<>(this, request);
                this.exchanges.add(exchange);
                return exchange;
            }

            private Window build() {
                return new Window(Flux.fromIterable(this.exchanges).map(Exchange.Builder::build));
            }
        }
    }

}
