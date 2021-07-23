/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import io.r2dbc.mssql.message.tds.Redirect;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.assertj.core.api.Assertions;
import org.reactivestreams.Publisher;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Test {@link Client} implementation.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public final class TestClient implements Client {

    public static final TestClient NO_OP = new TestClient(false, true, Flux.empty(), Optional.empty(), TransactionStatus.AUTO_COMMIT);


    private final boolean expectClose;

    private final boolean connected;

    private boolean closed;

    private final EmitterProcessor<Message> requestProcessor = EmitterProcessor.create(false);

    private final FluxSink<Message> requests = this.requestProcessor.sink();

    private final EmitterProcessor<Flux<Message>> responseProcessor = EmitterProcessor.create(false);

    private final TransactionStatus transactionStatus;

    private final Optional<Redirect> redirect;

    private TestClient(boolean expectClose, boolean connected, Flux<Window> windows, Optional<Redirect> redirect, TransactionStatus transactionStatus) {

        this.expectClose = expectClose;
        this.connected = connected;
        this.redirect = redirect;
        this.transactionStatus = transactionStatus;

        FluxSink<Flux<Message>> responses = this.responseProcessor.sink();

        Assert.requireNonNull(windows, "Windows must not be null")
            .map(window -> window.exchanges)
            .map(exchanges -> exchanges
                .concatMap(exchange ->

                    this.requestProcessor.zipWith(exchange.requests)
                        .handle((tuple, sink) -> {
                            Message actual = tuple.getT1();
                            Consumer<Message> expected = (Consumer) tuple.getT2();

                            try {
                                expected.accept(actual);
                            } catch (Throwable t) {
                                sink.error(t);
                            }
                        })
                        .thenMany(exchange.responses)))
            .subscribe(responses::next, responses::error, responses::complete);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Mono<Void> attention() {
        return Mono.empty();
    }

    @Override
    public Mono<Void> close() {
        return this.expectClose ? Mono.fromRunnable(() -> {
            this.closed = true;
        }) : Mono.error(new AssertionError("close called unexpectedly"));
    }

    public boolean isClosed() {
        return this.closed;
    }

    public Flux<Message> exchange(Publisher<? extends ClientMessage> requests, Predicate<Message> takeUntil) {

        Assert.requireNonNull(requests, "requests must not be null");

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
    public ConnectionContext getContext() {
        return new ConnectionContext();
    }

    @Override
    public Optional<Collation> getDatabaseCollation() {

        // windows-1252
        return Optional.of(Collation.from(13632521, 52));
    }

    @Override
    public Optional<String> getDatabaseVersion() {
        return Optional.of("1.2.3");
    }

    @Override
    public Optional<Redirect> getRedirect() {
        return this.redirect;
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

    @Override
    public boolean isConnected() {
        return this.connected;
    }

    public static final class Builder {

        private final List<Window.Builder<?>> windows = new ArrayList<>();

        private boolean expectClose = false;

        private boolean connected = true;

        private Optional<Redirect> redirect = Optional.empty();

        private TransactionStatus transactionStatus = TransactionStatus.AUTO_COMMIT;

        private Builder() {
        }

        public TestClient build() {
            return new TestClient(this.expectClose, this.connected, Flux.fromIterable(this.windows).map(Window.Builder::build), this.redirect, this.transactionStatus);
        }

        public Builder expectClose() {
            this.expectClose = true;
            return this;
        }

        public Builder withConnected(boolean connected) {
            this.connected = connected;
            return this;
        }

        public Builder withRedirect(Redirect redirect) {
            this.redirect = Optional.of(redirect);
            return this;
        }

        public Builder withTransactionStatus(TransactionStatus transactionStatus) {
            this.transactionStatus = Assert.requireNonNull(transactionStatus, "TransactionStatus must not be nuln");
            return this;
        }

        public Exchange.Builder<Builder> expectRequest(ClientMessage... requests) {
            Assert.requireNonNull(requests, "ClientMessage requests must not be null");

            Consumer[] consumers = Arrays.stream(requests).map(request -> {

                Consumer<ClientMessage> messageConsumer = actual -> Assertions.assertThat(actual).isEqualTo(request);
                return messageConsumer;
            }).toArray(i -> new Consumer[i]);

            return assertNextRequestWith(consumers);
        }

        public Exchange.Builder<Builder> assertNextRequestWith(Consumer<ClientMessage> request) {
            Assert.requireNonNull(request, "Client Consumer must not be null");

            return assertNextRequestWith(new Consumer[]{request});
        }

        public Exchange.Builder<Builder> assertNextRequestWith(Consumer<ClientMessage>... requests) {
            Assert.requireNonNull(requests, "Client Consumer must not be null");

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

        private final Flux<Consumer<? extends Message>> requests;

        private final Publisher<Message> responses;

        private Exchange(Flux<Consumer<? extends Message>> requests, Publisher<Message> responses) {
            this.requests = Assert.requireNonNull(requests, "Requests must not be null");
            this.responses = Assert.requireNonNull(responses, "Responses must not be null");
        }

        public static final class Builder<T> {

            private final T chain;

            private final Flux<Consumer<? extends Message>> requests;

            private Publisher<Message> responses;

            private Builder(T chain, Consumer<? extends Message>... requests) {
                this.chain = Assert.requireNonNull(chain, "Request chain must not be null");
                this.requests = Flux.just(Assert.requireNonNull(requests, "Requests must not be null"));
            }

            public T thenRespond(Message... responses) {
                Assert.requireNonNull(responses, "Responses must not be null");

                return thenRespond(Flux.just(responses));
            }

            T thenRespond(Publisher<Message> responses) {
                Assert.requireNonNull(responses, "Responses must not be null");

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
            this.exchanges = Assert.requireNonNull(exchanges, "Exchanges must not be null");
        }

        public static final class Builder<T> {

            private final T chain;

            private final List<Exchange.Builder<?>> exchanges = new ArrayList<>();

            private Builder(T chain) {
                this.chain = Assert.requireNonNull(chain, "Chain must not be null");
            }

            public T done() {
                return this.chain;
            }

            public Exchange.Builder<Builder<T>> expectRequest(ClientMessage request) {
                return assertNextRequestWith((Consumer<ClientMessage>) actual -> Assertions.assertThat(actual).isEqualTo(request));
            }

            public Exchange.Builder<Builder<T>> assertNextRequestWith(Consumer<ClientMessage> request) {

                Assert.requireNonNull(request, "Request must not be null");

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
