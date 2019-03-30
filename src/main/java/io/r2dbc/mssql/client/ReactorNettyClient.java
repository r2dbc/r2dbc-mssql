/*
 * Copyright 2018-2019 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.mssql.client.ssl.TdsSslHandler;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.header.PacketIdProvider;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.message.token.AbstractInfoToken;
import io.r2dbc.mssql.message.token.EnvChangeToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.FeatureExtAckToken;
import io.r2dbc.mssql.message.token.InfoToken;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.Assert;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * An implementation of a TDS client based on the Reactor Netty project.
 *
 * @see TcpClient
 */
public final class ReactorNettyClient implements Client {

    private static final Logger logger = LoggerFactory.getLogger(ReactorNettyClient.class);

    private final AtomicReference<ByteBufAllocator> byteBufAllocator = new AtomicReference<>();

    private final AtomicReference<Connection> connection = new AtomicReference<>();

    private final AtomicReference<TransactionDescriptor> transactionDescriptor = new AtomicReference<>(TransactionDescriptor.empty());

    private final AtomicReference<TransactionStatus> transactionStatus = new AtomicReference<>(TransactionStatus.AUTO_COMMIT);

    private final AtomicReference<Optional<Collation>> databaseCollation = new AtomicReference<>(Optional.empty());

    private final AtomicBoolean encryptionSupported = new AtomicBoolean();

    private final List<EnvironmentChangeListener> envChangeListeners = new ArrayList<>();

    private final Consumer<AbstractInfoToken> infoTokenConsumer = (token) -> {

        if (logger.isDebugEnabled()) {
            if (token.getClassification() == AbstractInfoToken.Classification.INFORMATIONAL) {
                logger.debug("Info: Code [{}] Severity [{}]: {}", token.getNumber(), token.getClassification(),
                    token.getMessage());
            } else {
                logger.debug("Warning: Code [{}] Severity [{}]: {}", token.getNumber(), token.getClassification(),
                    token.getMessage());
            }
        }
    };

    private final Consumer<Message> handleInfoToken = handleExact(InfoToken.class, infoTokenConsumer::accept);

    private final Consumer<Message> handleErrorToken = handleExact(ErrorToken.class, infoTokenConsumer::accept);

    private final Consumer<Message> handleEnvChange = handleExact(EnvChangeToken.class, (token) -> {

        EnvironmentChangeEvent event = new EnvironmentChangeEvent(token);

        for (EnvironmentChangeListener listener : this.envChangeListeners) {
            try {
                listener.onEnvironmentChange(event);
            } catch (Exception e) {
                logger.warn("Failed onEnvironmentChange() in {}", listener, e);
            }
        }
    });

    private final Consumer<Message> featureAckChange = handleExact(FeatureExtAckToken.class, (token) -> {

        for (FeatureExtAckToken.FeatureToken featureToken : token.getFeatureTokens()) {

            if (featureToken instanceof FeatureExtAckToken.ColumnEncryption) {
                this.encryptionSupported.set(true);
            }
        }
    });

    private final BiConsumer<Message, SynchronousSink<Message>> handleStateChange = handleMessage(Message.class,
        (message, sink) -> {

            ConnectionState connectionState = this.state.get();

            if (connectionState.canAdvance(message)) {
                ConnectionState nextState = connectionState.next(message, this.connection.get());
                if (this.state.compareAndSet(connectionState, nextState)) {
                    this.decodeFunction.set(nextState.decoder(this));
                } else {
                    sink.error(new ProtocolException(String.format("Cannot advance state from [%s]", connectionState)));
                }
            }

            sink.next(message);
        });

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final EmitterProcessor<ClientMessage> requestProcessor = EmitterProcessor.create(false);

    private final FluxSink<ClientMessage> requests = this.requestProcessor.sink();

    private final EmitterProcessor<Message> responseProcessor = EmitterProcessor.create(false);

    private final AtomicReference<ConnectionState> state = new AtomicReference<>(ConnectionState.PRELOGIN);

    private final AtomicReference<MessageDecoder> decodeFunction = new AtomicReference<>(ConnectionState.PRELOGIN.decoder(this));

    /**
     * Creates a new frame processor connected to a given TCP connection.
     *
     * @param connection the TCP connection
     */
    private ReactorNettyClient(Connection connection, List<EnvironmentChangeListener> envChangeListeners) {
        Assert.requireNonNull(connection, "Connection must not be null");

        FluxSink<Message> responses = this.responseProcessor.sink();

        StreamDecoder decoder = new StreamDecoder();

        this.byteBufAllocator.set(connection.outbound().alloc());
        this.connection.set(connection);
        this.envChangeListeners.addAll(envChangeListeners);
        this.envChangeListeners.add(new TransactionListener());
        this.envChangeListeners.add(new CollationListener());

        connection.inbound().receiveObject() //
            .concatMap(it -> {

                if (it instanceof ByteBuf) {

                    ByteBuf buffer = (ByteBuf) it;
                    return decoder.decode(buffer, this.decodeFunction.get());
                }

                if (it instanceof Message) {
                    return Mono.just((Message) it);
                }

                return Mono.error(new ProtocolException(String.format("Unexpected protocol message: [%s]", it)));
            }) //
            .as(it -> {
                if (logger.isDebugEnabled()) {
                    return it.doOnNext(message -> logger.debug("Response: {}", message));
                }
                return it;
            })
            .as(it -> {
                if (logger.isDebugEnabled()) {
                    return it.doOnNext(handleInfoToken).doOnNext(handleErrorToken);
                }
                return it;
            })
            .doOnError(message -> logger.warn("Error: {}", message.getMessage(), message)) //
            .handle(this.handleStateChange) //
            .doOnNext(m -> {
                this.handleEnvChange.accept(m);
                this.featureAckChange.accept(m);
            }) //
            .doOnError(ProtocolException.class, e -> {
                this.isClosed.set(true);
                connection.channel().close();
            })
            .subscribe(
                responses::next, responses::error, responses::complete);

        this.requestProcessor.doOnError(message -> {
            logger.warn("Error: {}", message.getMessage(), message);
            this.isClosed.set(true);
            connection.channel().close();
        }).as(it -> {
            if (logger.isDebugEnabled()) {
                return it.doOnNext(message -> logger.debug("Request: {}", message));
            }
            return it;
        })
            .concatMap(
                message -> connection.outbound().sendObject(message.encode(connection.outbound().alloc())))
            .subscribe();
    }

    /**
     * Creates a new frame processor connected to a given host.
     *
     * @param host the host to connect to
     * @param port the port to connect to
     */
    public static Mono<ReactorNettyClient> connect(String host, int port) {

        Assert.requireNonNull(host, "host must not be null");

        return connect(host, port, Duration.ofSeconds(30));
    }

    /**
     * Creates a new frame processor connected to a given host.
     *
     * @param host           the host to connect to
     * @param port           the port to connect to
     * @param connectTimeout the connect timeout
     */
    public static Mono<ReactorNettyClient> connect(String host, int port, Duration connectTimeout) {

        Assert.requireNonNull(connectTimeout, "connect timeout must not be null");
        Assert.requireNonNull(host, "host must not be null");

        return connect(host, port, connectTimeout, ConnectionProvider.newConnection());
    }

    /**
     * Creates a new frame processor connected to a given host.
     *
     * @param host               the host to connect to
     * @param port               the port to connect to
     * @param connectTimeout     the connect timeout
     * @param connectionProvider the connection provider resources
     */
    private static Mono<ReactorNettyClient> connect(String host, int port, Duration connectTimeout, ConnectionProvider connectionProvider) {

        Assert.requireNonNull(connectionProvider, "connectionProvider must not be null");
        Assert.requireNonNull(connectTimeout, "connect timeout must not be null");
        Assert.requireNonNull(host, "host must not be null");

        PacketIdProvider packetIdProvider = PacketIdProvider.atomic();

        TdsEncoder tdsEncoder = new TdsEncoder(packetIdProvider);

        Mono<? extends Connection> connection = TcpClient.create(connectionProvider)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(connectTimeout.toMillis()))
            .host(host)
            .port(port)
            .connect()
            .doOnNext(it -> {

                ChannelPipeline pipeline = it.channel().pipeline();
                InternalLogger logger = InternalLoggerFactory.getInstance(ReactorNettyClient.class);

                pipeline.addFirst(tdsEncoder.getClass().getName(), tdsEncoder);

                TdsSslHandler handler = new TdsSslHandler(packetIdProvider);
                pipeline.addAfter(tdsEncoder.getClass().getName(), handler.getClass().getName(), handler);

                if (logger.isDebugEnabled()) {
                    pipeline.addFirst(LoggingHandler.class.getSimpleName(),
                        new LoggingHandler(ReactorNettyClient.class, LogLevel.DEBUG));
                }
            });

        return connection.map(it -> new ReactorNettyClient(it, Collections.singletonList(tdsEncoder)));
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            Connection connection = this.connection.getAndSet(null);

            if (connection == null) {
                return Mono.empty();
            }

            return Mono.create(it -> {

                isClosed.set(true);

                connection.channel().disconnect().addListener((ChannelFutureListener) future ->
                {
                    if (future.isSuccess()) {
                        it.success();
                    } else {
                        it.error(future.cause());
                    }
                });
            });
        });
    }

    @Override
    public Flux<Message> exchange(Publisher<? extends ClientMessage> requests) {

        Assert.requireNonNull(requests, "Requests must not be null");

        return Flux.defer(() -> {
            if (this.isClosed.get()) {
                return Flux.error(new IllegalStateException("Cannot exchange messages because the connection is closed"));
            }

            return this.responseProcessor
                .doOnSubscribe(s -> Flux.from(requests).subscribe(this.requests::next, this.requests::error));
        });
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return this.byteBufAllocator.get();
    }

    @Override
    public TransactionDescriptor getTransactionDescriptor() {
        return this.transactionDescriptor.get();
    }

    @Override
    public TransactionStatus getTransactionStatus() {
        return this.transactionStatus.get();
    }

    @Override
    public Optional<Collation> getDatabaseCollation() {
        return this.databaseCollation.get();
    }

    @Override
    public boolean isColumnEncryptionSupported() {
        return this.encryptionSupported.get();
    }

    @SuppressWarnings("unchecked")
    private static <T extends Message> BiConsumer<Message, SynchronousSink<Message>> handleMessage(Class<T> type,
                                                                                                   BiConsumer<T, SynchronousSink<Message>> consumer) {
        return (message, sink) -> {
            if (type.isInstance(message)) {
                consumer.accept((T) message, sink);
            } else {
                sink.next(message);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T extends Message> Consumer<Message> handleExact(Class<T> type, Consumer<T> consumer) {
        return (message) -> {
            if (type == message.getClass()) {
                consumer.accept((T) message);
            }
        };
    }

    class TransactionListener implements EnvironmentChangeListener {

        @Override
        public void onEnvironmentChange(EnvironmentChangeEvent event) {

            EnvChangeToken token = event.getToken();

            if (token.getChangeType() == EnvChangeToken.EnvChangeType.BeginTx
                || token.getChangeType() == EnvChangeToken.EnvChangeType.EnlistDTC) {

                byte[] descriptor = token.getNewValue();

                if (descriptor.length != TransactionDescriptor.LENGTH) {
                    throw ProtocolException.invalidTds("Transaction descriptor length mismatch");
                }

                if (logger.isDebugEnabled()) {

                    String op;
                    if (token.getChangeType() == EnvChangeToken.EnvChangeType.BeginTx) {
                        op = "started";
                    } else {
                        op = "enlisted";
                    }

                    logger.debug(String.format("Transaction %s", op));
                }

                updateStatus(TransactionStatus.STARTED, TransactionDescriptor.from(descriptor));
            }

            if (token.getChangeType() == EnvChangeToken.EnvChangeType.CommitTx) {

                if (logger.isDebugEnabled()) {
                    logger.debug("Transaction committed");
                }

                updateStatus(TransactionStatus.EXPLICIT, TransactionDescriptor.empty());
            }

            if (token.getChangeType() == EnvChangeToken.EnvChangeType.RollbackTx) {

                if (logger.isDebugEnabled()) {
                    logger.debug("Transaction rolled back");
                }

                updateStatus(TransactionStatus.EXPLICIT, TransactionDescriptor.empty());
            }
        }

        private void updateStatus(TransactionStatus status, TransactionDescriptor descriptor) {
            transactionStatus.set(status);
            transactionDescriptor.set(descriptor);
        }
    }

    class CollationListener implements EnvironmentChangeListener {

        @Override
        public void onEnvironmentChange(EnvironmentChangeEvent event) {

            if (event.getToken().getChangeType() == EnvChangeToken.EnvChangeType.SQLCollation) {

                Collation collation = Collation.decode(Unpooled.wrappedBuffer(event.getToken().getNewValue()));
                databaseCollation.set(Optional.of(collation));
            }
        }
    }
}
