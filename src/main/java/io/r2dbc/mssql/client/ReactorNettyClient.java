/*
 * Copyright 2018-2020 the original author or authors.
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
import io.netty.channel.Channel;
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
import io.r2dbc.mssql.message.tds.Redirect;
import io.r2dbc.mssql.message.token.AbstractInfoToken;
import io.r2dbc.mssql.message.token.EnvChangeToken;
import io.r2dbc.mssql.message.token.FeatureExtAckToken;
import io.r2dbc.mssql.message.token.LoginAckToken;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * An implementation of a TDS client based on the Reactor Netty project.
 *
 * @see TcpClient
 */
public final class ReactorNettyClient implements Client {

    private static final Logger logger = Loggers.getLogger(ReactorNettyClient.class);

    private static final boolean DEBUG_ENABLED = logger.isDebugEnabled();

    private static final Supplier<MssqlConnectionClosedException> UNEXPECTED = () -> new MssqlConnectionClosedException("Connection unexpectedly closed");

    private static final Supplier<MssqlConnectionClosedException> EXPECTED = () -> new MssqlConnectionClosedException("Connection closed");

    private static final Supplier<MssqlConnectionClosedException> CLOSED = () -> new MssqlConnectionClosedException("Cannot exchange messages because the connection is closed");

    private final ConnectionContext context;

    private final ByteBufAllocator byteBufAllocator;

    private final Connection connection;

    private final TdsEncoder tdsEncoder;

    private final Consumer<EnvChangeToken> handleEnvChange;

    private final Consumer<FeatureExtAckToken> featureAckChange = (token) -> {

        for (FeatureExtAckToken.FeatureToken featureToken : token.getFeatureTokens()) {

            if (featureToken instanceof FeatureExtAckToken.ColumnEncryption) {
                this.encryptionSupported = true;
            }
        }
    };

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final EmitterProcessor<ClientMessage> requestProcessor = EmitterProcessor.create(false);

    private final FluxSink<ClientMessage> requests = this.requestProcessor.sink();

    private final EmitterProcessor<Message> responseProcessor = EmitterProcessor.create(false);

    private final TransactionListener transactionListener = new TransactionListener();

    private final CollationListener collationListener = new CollationListener();

    private final RedirectListener redirectListener = new RedirectListener();

    private final RequestQueue requestQueue;

    // May change during initialization. Values remain the same after connection initialization.

    private ConnectionState state = ConnectionState.PRELOGIN;

    private MessageDecoder decodeFunction = ConnectionState.PRELOGIN.decoder(this);

    private boolean encryptionSupported = false;

    private volatile Optional<Collation> databaseCollation = Optional.empty();

    private Optional<String> databaseVersion = Optional.empty();

    private volatile Optional<Redirect> redirect = Optional.empty();

    // May change during driver interaction, may be read on other threads.

    private volatile TransactionDescriptor transactionDescriptor = TransactionDescriptor.empty();

    private volatile TransactionStatus transactionStatus = TransactionStatus.AUTO_COMMIT;

    /**
     * Creates a new frame processor connected to a given TCP connection.
     *
     * @param connection        the TCP connection
     * @param connectionContext the connection context
     */
    private ReactorNettyClient(Connection connection, TdsEncoder tdsEncoder, ConnectionContext connectionContext) {
        Assert.requireNonNull(connection, "Connection must not be null");

        this.context = connectionContext;

        StreamDecoder decoder = new StreamDecoder();

        this.handleEnvChange = (token) -> {

            EnvironmentChangeEvent event = new EnvironmentChangeEvent(token);

            try {
                tdsEncoder.onEnvironmentChange(event);
                this.transactionListener.onEnvironmentChange(event);
                this.collationListener.onEnvironmentChange(event);
                this.redirectListener.onEnvironmentChange(event);
            } catch (Exception e) {
                logger.warn(this.context.getMessage("Failed onEnvironmentChange() in {}"), "", e);
            }
        };

        this.byteBufAllocator = connection.outbound().alloc();
        this.connection = connection;
        this.tdsEncoder = tdsEncoder;
        this.requestQueue = new RequestQueue(this.context);

        Consumer<Message> handleStateChange =
            (message) -> {

                if (message.getClass() == LoginAckToken.class) {
                    LoginAckToken loginAckToken = (LoginAckToken) message;
                    this.databaseVersion = Optional.of(loginAckToken.getVersion().toString());
                }

                ConnectionState connectionState = this.state;

                if (connectionState.canAdvance(message)) {

                    ConnectionState nextState = connectionState.next(message, connection);

                    this.state = nextState;
                    this.decodeFunction = nextState.decoder(this);
                }
            };

        SynchronousSink<Message> sink = new SynchronousSink<Message>() {

            @Override
            public void complete() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Context currentContext() {
                return ReactorNettyClient.this.requestProcessor.currentContext();
            }

            @Override
            public void error(Throwable e) {

                Throwable errorToUse = e;
                if (!(errorToUse instanceof R2dbcException)) {
                    errorToUse = new MssqlConnectionException(errorToUse);
                }

                ReactorNettyClient.this.responseProcessor.onError(errorToUse);
            }

            @Override
            public void next(Message message) {

                if (DEBUG_ENABLED) {
                    onInfoToken(message);
                }

                handleStateChange.accept(message);

                if (message.getClass() == EnvChangeToken.class) {
                    ReactorNettyClient.this.handleEnvChange.accept((EnvChangeToken) message);
                }

                if (message.getClass() == FeatureExtAckToken.class) {
                    ReactorNettyClient.this.featureAckChange.accept((FeatureExtAckToken) message);
                }

                ReactorNettyClient.this.responseProcessor.onNext(message);
            }
        };

        connection.inbound().receiveObject() //
            .doOnNext(it -> {

                if (it instanceof ByteBuf) {

                    ByteBuf buffer = (ByteBuf) it;
                    decoder.decode(buffer, this.decodeFunction, sink);
                    return;
                }

                if (it instanceof Message) {
                    sink.next((Message) it);
                    return;
                }

                throw ProtocolException.unsupported(String.format("Unexpected protocol message: [%s]", it));
            })
            .onErrorResume(this::resumeError)
            .subscribe(new CoreSubscriber<Object>() {

                @Override
                public Context currentContext() {
                    return ReactorNettyClient.this.responseProcessor.currentContext();
                }

                @Override
                public void onSubscribe(Subscription s) {
                    ReactorNettyClient.this.responseProcessor.onSubscribe(s);
                }

                @Override
                public void onNext(Object message) {
                }

                @Override
                public void onError(Throwable t) {
                    sink.error(t);
                }

                @Override
                public void onComplete() {
                    handleClose();
                }
            });

        this.requestProcessor
            .concatMap(
                message -> {

                    if (DEBUG_ENABLED) {
                        logger.debug(this.context.getMessage("Request: {}"), message);
                    }

                    Object encoded = message.encode(connection.outbound().alloc(), this.tdsEncoder.getPacketSize());

                    if (encoded instanceof Publisher) {
                        return connection.outbound().sendObject((Publisher) encoded);
                    }

                    return connection.outbound().sendObject(encoded);
                })
            .onErrorResume(this::resumeError)
            .doAfterTerminate(this::handleClose)
            .subscribe();
    }

    @SuppressWarnings("unchecked")
    private <T> Mono<T> resumeError(Throwable throwable) {

        handleConnectionError(throwable);
        this.requestProcessor.onComplete();

        logger.error(this.context.getMessage("Error: {}"), throwable.getMessage(), throwable);

        return (Mono<T>) close();
    }

    private void onInfoToken(Message message) {
        logger.debug(this.context.getMessage("Response: {}"), message);

        if (message instanceof AbstractInfoToken) {
            AbstractInfoToken token = (AbstractInfoToken) message;
            if (token.getClassification() == AbstractInfoToken.Classification.INFORMATIONAL) {
                logger.debug(this.context.getMessage("Info: Code [{}] Severity [{}]: {}"), token.getNumber(), token.getClassification(),
                    token.getMessage());
            } else {
                logger.debug(this.context.getMessage("Warning: Code [{}] Severity [{}]: {}"), token.getNumber(), token.getClassification(),
                    token.getMessage());
            }
        }
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

        return connect(new ClientConfiguration() {

            @Override
            public String getHost() {
                return host;
            }

            @Override
            public int getPort() {
                return port;
            }

            @Override
            public Duration getConnectTimeout() {
                return connectTimeout;
            }

            @Override
            public ConnectionProvider getConnectionProvider() {
                return ConnectionProvider.newConnection();
            }

            @Override
            public boolean isSslEnabled() {
                return false;
            }

            @Override
            public String getHostNameInCertificate() {
                return host;
            }
        }, null, null);
    }

    /**
     * Creates a new frame processor connected to {@link ClientConfiguration}.
     *
     * @param configuration   the client configuration
     * @param applicationName
     * @param connectionId
     */
    public static Mono<ReactorNettyClient> connect(ClientConfiguration configuration, @Nullable String applicationName, @Nullable UUID connectionId) {

        Assert.requireNonNull(configuration, "configuration must not be null");

        ConnectionContext connectionContext = new ConnectionContext(applicationName, connectionId);
        logger.debug(connectionContext.getMessage("connect()"));

        PacketIdProvider packetIdProvider = PacketIdProvider.atomic();

        TdsEncoder tdsEncoder = new TdsEncoder(packetIdProvider);

        Mono<? extends Connection> connection = TcpClient.create(configuration.getConnectionProvider())
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(configuration.getConnectTimeout().toMillis()))
            .host(configuration.getHost())
            .port(configuration.getPort())
            .connect()
            .doOnNext(it -> {

                ChannelPipeline pipeline = it.channel().pipeline();
                pipeline.addFirst(tdsEncoder.getClass().getName(), tdsEncoder);

                TdsSslHandler handler = new TdsSslHandler(packetIdProvider, configuration, connectionContext.withChannelId(it.channel().toString()));
                pipeline.addAfter(tdsEncoder.getClass().getName(), handler.getClass().getName(), handler);

                InternalLogger logger = InternalLoggerFactory.getInstance(ReactorNettyClient.class);
                if (logger.isTraceEnabled()) {
                    pipeline.addFirst(LoggingHandler.class.getSimpleName(),
                        new LoggingHandler(ReactorNettyClient.class, LogLevel.TRACE));
                }
            });

        return connection.map(it -> new ReactorNettyClient(it, tdsEncoder, connectionContext.withChannelId(it.channel().toString())));
    }

    @Override
    public Mono<Void> close() {

        logger.debug(this.context.getMessage("close()"));

        return Mono.defer(() -> {

            logger.debug(this.context.getMessage("close(subscribed)"));

            if (this.isClosed.compareAndSet(false, true)) {
                this.connection.dispose();
                return this.connection.onDispose();
            }

            return Mono.empty();
        });
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return this.byteBufAllocator;
    }

    @Override
    public ConnectionContext getContext() {
        return this.context;
    }

    @Override
    public Optional<Collation> getDatabaseCollation() {
        return this.databaseCollation;
    }

    @Override
    public Optional<String> getDatabaseVersion() {
        return this.databaseVersion;
    }

    @Override
    public Optional<Redirect> getRedirect() {
        return this.redirect;
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
    public boolean isColumnEncryptionSupported() {
        return this.encryptionSupported;
    }

    @Override
    public boolean isConnected() {

        if (this.isClosed.get()) {
            return false;
        }

        if (this.requestProcessor.isDisposed()) {
            return false;
        }

        Channel channel = this.connection.channel();
        return channel.isOpen();
    }

    @Override
    public Flux<Message> exchange(Publisher<? extends ClientMessage> requests, Predicate<Message> isLastResponseFrame) {

        Assert.requireNonNull(requests, "Requests must not be null");

        if (DEBUG_ENABLED) {
            logger.debug(this.context.getMessage("exchange()"));
        }

        ExchangeRequest exchangeRequest = new ExchangeRequest();

        Flux<Message> handle = Mono.<Flux<Message>>create(sink -> {

            if (DEBUG_ENABLED) {
                logger.debug(this.context.getMessage("exchange(subscribed)"));
            }

            if (!isConnected()) {
                sink.error(CLOSED.get());
            }

            Flux<Message> requestMessages = this.responseProcessor
                .doOnSubscribe(s -> {
                    Flux.from(requests).subscribe(t -> {

                        if (!isConnected()) {
                            sink.error(CLOSED.get());
                            return;
                        }

                        this.requests.next(t);
                    }, this.requests::error, () -> {

                        if (!isConnected()) {
                            sink.error(CLOSED.get());
                        }
                    });
                });

            try {
                exchangeRequest.submit(this.requestQueue, sink, requestMessages);
            } catch (Exception e) {
                sink.error(e);
            }

        }).flatMapMany(Function.identity()).handle((message, sink) -> {

            sink.next(message);

            if (isLastResponseFrame.test(message)) {
                exchangeRequest.complete();
                sink.complete();
            }
        });

        return handle.doAfterTerminate(this.requestQueue).doOnCancel(() -> {

            if (!exchangeRequest.isComplete()) {
                logger.error("Exchange cancelled while exchange is active. This is likely a bug leading to unpredictable outcome.");
            }
        });
    }

    private void handleClose() {
        if (this.isClosed.compareAndSet(false, true)) {
            logger.warn(ReactorNettyClient.this.context.getMessage("Connection has been closed by peer"));
            drainError(UNEXPECTED);
        } else {
            drainError(EXPECTED);
        }
    }

    private void handleConnectionError(Throwable error) {
        drainError(() -> new MssqlConnectionException(error));
    }

    private void drainError(Supplier<? extends Throwable> supplier) {

        Sinkable receiver;
        while ((receiver = this.requestQueue.poll()) != null) {
            receiver.onError(supplier.get());
        }

        this.responseProcessor.onError(supplier.get());
    }

    /**
     * Request queue to collect incoming exchange requests.
     * <p>Submission conditionally queues requests if an ongoing exchange was active by the time of subscription.
     * Drains queued commands on exchange completion if there are queued commands or disable active flag.
     */
    static class RequestQueue implements Runnable {

        private final Queue<Sinkable> requestQueue = Queues.<Sinkable>small().get();

        private final AtomicBoolean active = new AtomicBoolean();

        private final ConnectionContext context;

        RequestQueue(ConnectionContext context) {
            this.context = context;
        }

        @Nullable
        public Sinkable poll() {
            return this.requestQueue.poll();
        }

        @Override
        public void run() {

            Sinkable nextCommand = this.requestQueue.poll();

            if (nextCommand != null) {

                if (DEBUG_ENABLED) {
                    logger.debug(this.context.getMessage("Initiating queued exchange"));
                }

                nextCommand.onSuccess();
                return;
            }

            if (DEBUG_ENABLED) {
                logger.debug(this.context.getMessage("Conversation complete"));
            }

            this.active.compareAndSet(true, false);
        }

        /**
         * Submit a {@code exchangeRequest}. Requests are either executed directly (without an active exchange) or queued (if another exchange is currently active).
         *
         * @param exchangeRequest
         */
        void submit(Sinkable exchangeRequest) {

            if (this.active.compareAndSet(false, true)) {

                if (DEBUG_ENABLED) {
                    logger.debug(this.context.getMessage("Initiating exchange"));
                }

                exchangeRequest.onSuccess();
            } else {

                if (DEBUG_ENABLED) {
                    logger.debug(this.context.getMessage("Queueing exchange"));
                }

                if (!this.requestQueue.offer(exchangeRequest)) {
                    throw new IllegalStateException("Request queue is full");
                }

                drainRequestQueue();
            }
        }

        void drainRequestQueue() {

            if (this.active.compareAndSet(false, true)) {

                Sinkable runnable = this.requestQueue.poll();

                if (runnable != null) {
                    runnable.onSuccess();
                } else {
                    this.active.compareAndSet(true, false);
                }
            }
        }
    }

    /**
     * Ensure a command request is submitted and subscribed to only once.
     */
    static class ExchangeRequest {

        private static final AtomicIntegerFieldUpdater<ExchangeRequest> COMPLETED = AtomicIntegerFieldUpdater.newUpdater(ExchangeRequest.class, "completed");

        private static final AtomicIntegerFieldUpdater<ExchangeRequest> SUBMITTED = AtomicIntegerFieldUpdater.newUpdater(ExchangeRequest.class, "submitted");

        // access via COMPLETED
        private volatile int completed = 0;

        // access via SUBMITTED
        private volatile int submitted = 0;

        private volatile Sinkable sinkable;

        public void complete() {
            COMPLETED.set(this, 1);
        }

        public boolean isComplete() {
            return COMPLETED.get(this) == 1;
        }

        void submit(RequestQueue queue, MonoSink<Flux<Message>> sink, Flux<Message> requestMessages) {

            if (!SUBMITTED.compareAndSet(this, 0, 1)) {
                throw new IllegalStateException("Client exchange can be subscribed only once");
            }

            queue.submit(new Sinkable() {

                @Override
                public void onSuccess() {
                    sink.success(requestMessages);
                }

                @Override
                public void onError(Throwable throwable) {
                    sink.error(throwable);
                }
            });
        }
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

                if (DEBUG_ENABLED) {

                    String op;
                    if (token.getChangeType() == EnvChangeToken.EnvChangeType.BeginTx) {
                        op = "started";
                    } else {
                        op = "enlisted";
                    }

                    logger.debug(String.format(ReactorNettyClient.this.context.getMessage("Transaction %s"), op));
                }

                updateStatus(TransactionStatus.STARTED, TransactionDescriptor.from(descriptor));
            }

            if (token.getChangeType() == EnvChangeToken.EnvChangeType.CommitTx) {

                if (DEBUG_ENABLED) {
                    logger.debug(ReactorNettyClient.this.context.getMessage("Transaction committed"));
                }

                updateStatus(TransactionStatus.EXPLICIT, TransactionDescriptor.empty());
            }

            if (token.getChangeType() == EnvChangeToken.EnvChangeType.RollbackTx) {

                if (DEBUG_ENABLED) {
                    logger.debug(ReactorNettyClient.this.context.getMessage("Transaction rolled back"));
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

    class RedirectListener implements EnvironmentChangeListener {

        @Override
        public void onEnvironmentChange(EnvironmentChangeEvent event) {

            if (event.getToken().getChangeType() == EnvChangeToken.EnvChangeType.Routing) {

                Redirect redirect = Redirect.decode(Unpooled.wrappedBuffer(event.getToken().getNewValue()));
                ReactorNettyClient.this.redirect = Optional.of(redirect);
            }
        }
    }

    interface Sinkable {

        void onSuccess();

        void onError(Throwable throwable);

    }

    static class MssqlConnectionClosedException extends R2dbcNonTransientResourceException {

        public MssqlConnectionClosedException(String reason) {
            super(reason);
        }
    }

    static class MssqlConnectionException extends R2dbcNonTransientResourceException {

        public MssqlConnectionException(Throwable cause) {
            super(cause);
        }
    }
}
