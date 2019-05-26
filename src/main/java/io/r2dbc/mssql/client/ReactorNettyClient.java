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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * An implementation of a TDS client based on the Reactor Netty project.
 *
 * @see TcpClient
 */
public final class ReactorNettyClient implements Client {

    private static final Logger logger = LoggerFactory.getLogger(ReactorNettyClient.class);

    private final ByteBufAllocator byteBufAllocator;

    private final Connection connection;

    private final TdsEncoder tdsEncoder;

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

    private final Consumer<Message> handleInfoToken = handleExact(InfoToken.class, this.infoTokenConsumer::accept);

    private final Consumer<Message> handleErrorToken = handleExact(ErrorToken.class, this.infoTokenConsumer::accept);

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
                this.encryptionSupported = true;
            }
        }
    });

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final EmitterProcessor<ClientMessage> requestProcessor = EmitterProcessor.create(false);

    private final FluxSink<ClientMessage> requests = this.requestProcessor.sink();

    private final EmitterProcessor<Message> responseProcessor = EmitterProcessor.create(false);

    private ConnectionState state = ConnectionState.PRELOGIN;

    private MessageDecoder decodeFunction = ConnectionState.PRELOGIN.decoder(this);

    private boolean encryptionSupported = false;

    private volatile TransactionDescriptor transactionDescriptor = TransactionDescriptor.empty();

    private volatile TransactionStatus transactionStatus = TransactionStatus.AUTO_COMMIT;

    private volatile Optional<Collation> databaseCollation = Optional.empty();

    /**
     * Creates a new frame processor connected to a given TCP connection.
     *
     * @param connection the TCP connection
     */
    private ReactorNettyClient(Connection connection, TdsEncoder tdsEncoder) {
        Assert.requireNonNull(connection, "Connection must not be null");

        FluxSink<Message> responses = this.responseProcessor.sink();

        StreamDecoder decoder = new StreamDecoder();

        this.byteBufAllocator = connection.outbound().alloc();
        this.connection = connection;
        this.tdsEncoder = tdsEncoder;

        this.envChangeListeners.add(tdsEncoder);
        this.envChangeListeners.add(new TransactionListener());
        this.envChangeListeners.add(new CollationListener());

        connection.addHandlerFirst(new ChannelInboundHandlerAdapter() {

            @Override
            public void channelInactive(ChannelHandlerContext ctx) throws Exception {

                // Server has closed the connection without us wanting to close it
                // Typically happens if we send data asynchronously (i.e. previous command didn't complete).
                if (ReactorNettyClient.this.isClosed.compareAndSet(false, true)) {
                    logger.warn("Connection has been closed by peer");
                }

                super.channelInactive(ctx);
            }
        });

        BiConsumer<Message, SynchronousSink<Message>> handleStateChange = handleMessage(Message.class,
            (message, sink) -> {

                ConnectionState connectionState = this.state;

                if (connectionState.canAdvance(message)) {

                    ConnectionState nextState = connectionState.next(message, connection);

                    this.state = nextState;
                    this.decodeFunction = nextState.decoder(this);
                }

                sink.next(message);
            });

        connection.inbound().receiveObject() //
            .concatMap(it -> {

                if (it instanceof ByteBuf) {

                    ByteBuf buffer = (ByteBuf) it;
                    return decoder.decode(buffer, this.decodeFunction);
                }

                if (it instanceof Message) {
                    return Mono.just((Message) it);
                }

                return Mono.error(ProtocolException.unsupported(String.format("Unexpected protocol message: [%s]", it)));
            }) //
            .as(it -> {
                if (logger.isDebugEnabled()) {
                    return it.doOnNext(message -> logger.debug("Response: {}", message));
                }
                return it;
            })
            .as(it -> {
                if (logger.isDebugEnabled()) {
                    return it.doOnNext(this.handleInfoToken).doOnNext(this.handleErrorToken);
                }
                return it;
            })
            .doOnError(message -> logger.warn("Error: {}", message.getMessage(), message)) //
            .handle(handleStateChange) //
            .doOnNext(m -> {
                this.handleEnvChange.accept(m);
                this.featureAckChange.accept(m);
            }) //
            .doOnError(ProtocolException.class, e -> {
                logger.warn("Error: {}", e.getMessage(), e);
                this.isClosed.set(true);
                connection.channel().close();
            })
            .subscribe(
                responses::next, responses::error, responses::complete);

        this.requestProcessor.as(it -> {
            if (logger.isDebugEnabled()) {
                return it.doOnNext(message -> logger.debug("Request: {}", message));
            }
            return it;
        })
            .concatMap(
                message -> connection.outbound().sendObject(message.encode(connection.outbound().alloc(), this.tdsEncoder.getPacketSize())))
            .doOnError(throwable -> {
                logger.warn("Error: {}", throwable.getMessage(), throwable);
                this.isClosed.set(true);
                connection.channel().close();
            })
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

        logger.debug("connect()");

        PacketIdProvider packetIdProvider = PacketIdProvider.atomic();

        TdsEncoder tdsEncoder = new TdsEncoder(packetIdProvider);

        Mono<? extends Connection> connection = TcpClient.create(connectionProvider)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(connectTimeout.toMillis()))
            .host(host)
            .port(port)
            .connect()
            .doOnNext(it -> {

                ChannelPipeline pipeline = it.channel().pipeline();
                pipeline.addFirst(tdsEncoder.getClass().getName(), tdsEncoder);

                TdsSslHandler handler = new TdsSslHandler(packetIdProvider);
                pipeline.addAfter(tdsEncoder.getClass().getName(), handler.getClass().getName(), handler);

                InternalLogger logger = InternalLoggerFactory.getInstance(ReactorNettyClient.class);
                if (logger.isTraceEnabled()) {
                    pipeline.addFirst(LoggingHandler.class.getSimpleName(),
                        new LoggingHandler(ReactorNettyClient.class, LogLevel.TRACE));
                }
            });

        return connection.map(it -> new ReactorNettyClient(it, tdsEncoder));
    }

    @Override
    public Mono<Void> close() {

        logger.debug("close()");

        return Mono.defer(() -> {

            logger.debug("close(subscribed)");

            return Mono.create(it -> {

                if (this.isClosed.compareAndSet(false, true)) {

                    this.connection.channel().disconnect().addListener((ChannelFutureListener) future ->
                    {
                        if (future.isSuccess()) {
                            it.success();
                        } else {
                            it.error(future.cause());
                        }
                    });
                } else {
                    it.success();
                }
            });
        });
    }

    @Override
    public Flux<Message> exchange(Publisher<? extends ClientMessage> requests) {

        Assert.requireNonNull(requests, "Requests must not be null");

        logger.debug("exchange()");

        return Flux.defer(() -> {

            logger.debug("exchange(subscribed)");

            if (this.isClosed.get()) {
                return Flux.error(new IllegalStateException("Cannot exchange messages because the connection is closed"));
            }

            return this.responseProcessor
                .doOnSubscribe(s -> Flux.from(requests).subscribe(this.requests::next, this.requests::error));
        });
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return this.byteBufAllocator;
    }

    @Override
    public TransactionDescriptor getTransactionDescriptor() {
        return this.transactionDescriptor;
    }

    @Override
    public TransactionStatus getTransactionStatus() {
        return this.transactionStatus;
    }

    @Override
    public Optional<Collation> getDatabaseCollation() {
        return this.databaseCollation;
    }

    @Override
    public boolean isColumnEncryptionSupported() {
        return this.encryptionSupported;
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
            ReactorNettyClient.this.transactionStatus = status;
            ReactorNettyClient.this.transactionDescriptor = descriptor;
        }
    }

    class CollationListener implements EnvironmentChangeListener {

        @Override
        public void onEnvironmentChange(EnvironmentChangeEvent event) {

            if (event.getToken().getChangeType() == EnvChangeToken.EnvChangeType.SQLCollation) {

                Collation collation = Collation.decode(Unpooled.wrappedBuffer(event.getToken().getNewValue()));
                ReactorNettyClient.this.databaseCollation = Optional.of(collation);
            }
        }
    }
}
