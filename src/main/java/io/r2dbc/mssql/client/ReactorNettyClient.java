/*
 * Copyright 2018-2022 the original author or authors.
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
import io.netty.handler.ssl.SslContext;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.mssql.client.ssl.SslHandlerFactory;
import io.r2dbc.mssql.client.ssl.TdsSslHandler;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.header.PacketIdProvider;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.message.tds.Redirect;
import io.r2dbc.mssql.message.token.*;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.*;
import reactor.netty.Connection;
import reactor.netty.NettyOutbound;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.SslProvider;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpSslContextSpec;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import javax.annotation.Nullable;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import java.util.function.Function;
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

    private final RequestSink requestSink;

    private final Sinks.Many<Message> responseProcessor = Sinks.many().multicast().onBackpressureBuffer(512, false);

    private final TransactionListener transactionListener = new TransactionListener();

    private final CollationListener collationListener = new CollationListener();

    private final RedirectListener redirectListener = new RedirectListener();

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
    ReactorNettyClient(Connection connection, TdsEncoder tdsEncoder, ConnectionContext connectionContext) {

        Assert.requireNonNull(connection, "Connection must not be null");
        Assert.state(this.responseProcessor.asFlux() instanceof Subscriber, () -> "Response processor " + this.responseProcessor + " is not a Subscriber. Cannot proceed.");

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

        AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
        SynchronousSink<Message> sink = new SynchronousSink<Message>() {

            @Override
            public void complete() {
                throw new UnsupportedOperationException();
            }

            @Deprecated
            @Override
            public Context currentContext() {
                return Context.empty();
            }

            @Override
            public ContextView contextView() {
                return Context.empty();
            }

            @Override
            public void error(Throwable e) {

                Throwable errorToUse = e;
                if (!(errorToUse instanceof R2dbcException)) {
                    errorToUse = new MssqlConnectionException(errorToUse);
                }

                ReactorNettyClient.this.responseProcessor.emitError(errorToUse, Sinks.EmitFailureHandler.FAIL_FAST);
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

                Subscription subscription = subscriptionRef.get();
                if (AbstractDoneToken.isAttentionAck(message)) {

                    long current;
                    do {
                        current = ReactorNettyClient.this.requestSink.getAttentionPropagation();

                        if (current == 0) {
                            if (DEBUG_ENABLED) {
                                logger.debug(ReactorNettyClient.this.context.getMessage("Swallowing attention acknowledged, no pending requests: {}. "), message);
                            }

                            // update demand for dropped next signal
                            if (subscription != null) {
                                subscription.request(1);
                            }
                            return;
                        }

                    } while (!ReactorNettyClient.this.requestSink.compareAndSetAttentionPropagation(current, current - 1));
                }

                long attentionPropagation = ReactorNettyClient.this.requestSink.getAttentionPropagation();

                if (attentionPropagation > 0 && !AbstractDoneToken.isAttentionAck(message)) {
                    if (DEBUG_ENABLED) {
                        logger.debug(ReactorNettyClient.this.context.getMessage("Discard message {}. Draining frames until attention acknowledgement."), message);
                    }
                    // release reference-counted frames (e.g. rows) that are dropped while draining
                    ReferenceCountUtil.release(message);
                    // update demand for dropped next signal
                    if (subscription != null) {
                        subscription.request(1);
                    }
                    return;
                }

                ReactorNettyClient.this.responseProcessor.emitNext(message, Sinks.EmitFailureHandler.FAIL_FAST);
            }
        };

        connection.inbound().receiveObject() //
            .concatMapIterable(it -> {

                if (it instanceof ByteBuf) {

                    ByteBuf buffer = (ByteBuf) it;
                    return decoder.decode(buffer, this.decodeFunction);
                }

                if (it instanceof Message) {
                    return Collections.singleton((Message) it);
                }

                throw ProtocolException.unsupported(String.format("Unexpected protocol message: [%s]", it));
            })
            .onErrorResume(this::resumeError)
            .subscribe(new CoreSubscriber<Message>() {

                @Override
                public void onSubscribe(Subscription s) {
                    subscriptionRef.set(s);

                    ((Subscriber<?>) ReactorNettyClient.this.responseProcessor.asFlux()).onSubscribe(s);
                }

                @Override
                public void onNext(Message message) {
                    sink.next(message);
                }

                @Override
                public void onError(Throwable t) {
                    decoder.dispose();
                    sink.error(t);
                }

                @Override
                public void onComplete() {
                    decoder.dispose();
                    handleClose();
                }
            });

        this.requestSink = new RequestSink(connectionContext);
        this.requestSink
            .asFlux()
            .concatMap(
                message -> {

                    Object encoded = encodeForSend(message);

                    NettyOutbound nettyOutbound = encoded instanceof Publisher
                        ? connection.outbound().sendObject((Publisher) encoded)
                        : connection.outbound().sendObject(encoded);

                    // An Attention travels through the same writer as the request it cancels.
                    if (message instanceof Attention && this.requestSink.getOutstandingRequests() != 0) {
                        return Mono.from(nettyOutbound).doOnSuccess(v -> this.requestSink.incrementAttentionPropagation());
                    }

                    return nettyOutbound;
                })
            .onErrorResume(this::resumeError)
            .doAfterTerminate(this::handleClose)
            .subscribe();
    }

    private Object encodeForSend(ClientMessage message) {

        if (DEBUG_ENABLED) {
            logger.debug(this.context.getMessage("Request: {}"), message);
        }

        return message.encode(this.connection.outbound().alloc(), this.tdsEncoder.getPacketSize());
    }

    @SuppressWarnings("unchecked")
    private <T> Mono<T> resumeError(Throwable throwable) {

        logger.error(this.context.getMessage("Error: {}"), throwable.getMessage(), throwable);

        // Terminate with the actual cause first. Completing the request sink below
        // synchronously triggers the outbound termination hook (handleClose) on this same thread, which
        // would otherwise win the race and terminate the response processor with a generic "closed"
        // error, masking this cause.
        handleConnectionError(throwable);

        this.requestSink.emitComplete((signalType, emitResult) -> {

            if (emitResult.isFailure()) {
                logger.error(this.context.getMessage("Error: {}"), emitResult);
            }

            return false;
        });

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
            public boolean isTcpKeepAlive() {
                return false;
            }

            @Override
            public boolean isTcpNoDelay() {
                return true;
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
            public SslContext getSslContext() {
                return SslProvider.builder()
                    .sslContext(TcpSslContextSpec.forClient())
                    .build().getSslContext();
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
            .option(ChannelOption.SO_KEEPALIVE, configuration.isTcpKeepAlive())
            .option(ChannelOption.TCP_NODELAY, configuration.isTcpNoDelay())
            .host(configuration.getHost())
            .port(configuration.getPort())
            .connect()
            .doOnNext(it -> {

                SslHandlerFactory sslHandlerFactory = SslHandlerFactory.create(configuration, ClientConfiguration::getSslTunnelConfiguration);
                ChannelPipeline pipeline = it.channel().pipeline();

                if (sslHandlerFactory.isSslEnabled()) {
                    logger.debug(connectionContext.getMessage("Enabling SSL tunnel"));
                    try {
                        pipeline.addFirst("sslTunnel", sslHandlerFactory.createSslHandler(it.channel().alloc()));
                    } catch (GeneralSecurityException e) {
                        it.channel().close();
                        throw new IllegalStateException("Cannot configure SSL tunnel", e);
                    }
                    pipeline.addAfter("sslTunnel", tdsEncoder.getClass().getName(), tdsEncoder);
                } else {
                    pipeline.addFirst(tdsEncoder.getClass().getName(), tdsEncoder);
                }

                TdsSslHandler handler = new TdsSslHandler(packetIdProvider, configuration, connectionContext.withChannelId(it.channel().toString()));
                pipeline.addAfter(tdsEncoder.getClass().getName(), handler.getClass().getName(), handler);

                InternalLogger logger = InternalLoggerFactory.getInstance(ReactorNettyClient.class);
                if (logger.isTraceEnabled()) {
                    pipeline.addBefore(tdsEncoder.getClass().getName(), LoggingHandler.class.getSimpleName(),
                        new LoggingHandler(ReactorNettyClient.class, LogLevel.TRACE));
                }
            });

        return connection.map(it -> new ReactorNettyClient(it, tdsEncoder, connectionContext.withChannelId(it.channel().toString())));
    }

    @Override
    public Mono<Void> attention() {
        return Mono.fromRunnable(() -> this.requestSink.emitNext(Attention.create(1, getTransactionDescriptor())));
    }

    @Override
    public Mono<Void> close() {

        logger.debug(this.context.getMessage("close()"));

        return Mono.defer(() -> {

            logger.debug(this.context.getMessage("close(subscribed)"));

            if (this.requestSink.close()) {
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

        if (this.requestSink.isClosed()) {
            return false;
        }

        Channel channel = this.connection.channel();
        return channel.isOpen();
    }

    @Override
    public Flux<Message> exchange(Publisher<? extends ClientMessage> requests, Conversation conversation) {

        Assert.requireNonNull(conversation, "Conversation must not be null");
        Assert.requireNonNull(requests, "Requests must not be null");

        if (DEBUG_ENABLED) {
            logger.debug(this.context.getMessage("exchange()"));
        }

        return new ExchangeLifecycle(this, requests, conversation).exchange();
    }


    private void handleClose() {
        if (this.requestSink.close()) {
            if (terminateConnection(UNEXPECTED)) {
                logger.warn(this.context.getMessage("Connection has been closed by peer"));
            }
        } else {
            terminateConnection(EXPECTED);
        }
    }

    private void handleConnectionError(Throwable error) {
        terminateConnection(() -> {
            if (this.state == ConnectionState.POST_LOGIN) {
                return new MssqlConnectionException(error);
            }
            return new MssqlConnectionException("Cannot connect to server", error);
        });
    }

    /**
     * Terminate request admission and the response processor with the supplied error.
     * <p>The first terminal reason wins.
     *
     * @return {@code true} if this call delivered the terminal error, {@code false} if already terminated.
     */
    private boolean terminateConnection(Supplier<? extends Throwable> supplier) {

        Throwable failure = supplier.get();

        if (!this.requestSink.terminate(failure)) {
            return false;
        }

        this.responseProcessor.emitError(failure, Sinks.EmitFailureHandler.FAIL_FAST);
        return true;
    }

    /**
     * Request sink accepting queued {@link Sinkable} exchanges and direct {@link ClientMessage} emission.
     */
    static class RequestSink {

        private static final Sinks.EmitFailureHandler EMIT_BUSY_LOOP = Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(5));

        private final Sinks.Many<ClientMessage> requestSink = Sinks.many().unicast().onBackpressureBuffer();

        private final AtomicBoolean isClosed = new AtomicBoolean(false);

        private final AtomicLong attentionPropagation = new AtomicLong();

        private final AtomicLong outstandingRequests = new AtomicLong();

        private final RequestQueue requestQueue;

        public RequestSink(ConnectionContext context) {
            this.requestQueue = new RequestQueue(context);
        }

        /**
         * Queue.
         */
        public void submit(Sinkable exchange) {
            this.requestQueue.submit(exchange);
        }

        public Flux<ClientMessage> asFlux() {
            return this.requestSink.asFlux();
        }

        /**
         * Emit a value to the request sink.
         */
        public void emitNext(ClientMessage message) {
            this.requestSink.emitNext(message, EMIT_BUSY_LOOP);
        }

        /**
         * Emit a completion to the request sink.
         */
        public void emitComplete(Sinks.EmitFailureHandler handler) {
            this.requestSink.emitComplete(handler);
        }

        /**
         * Emit an error to the request sink.
         */
        public void emitError(Throwable throwable) {
            this.requestSink.emitError(throwable, Sinks.EmitFailureHandler.FAIL_FAST);
        }

        /**
         * Close the sink and return {@code true} if the sink was closed with this call to prevent races.
         */
        public boolean close() {
            return this.isClosed.compareAndSet(false, true);
        }

        /**
         * Return {@code true} if the sink is closed.
         */
        public boolean isClosed() {
            return this.isClosed.get();
        }

        public long getAttentionPropagation() {
            return this.attentionPropagation.get();
        }

        public long incrementAttentionPropagation() {
            return this.attentionPropagation.incrementAndGet();
        }

        public boolean compareAndSetAttentionPropagation(long expectedValue, long newValue) {
            return this.attentionPropagation.compareAndSet(expectedValue, newValue);
        }

        public long getOutstandingRequests() {
            return this.outstandingRequests.longValue();
        }

        public long incrementOutstandingRequests() {
            return this.outstandingRequests.incrementAndGet();
        }

        public void decrementOutstandingRequests() {
            this.outstandingRequests.decrementAndGet();
        }

        public boolean terminate(Throwable failure) {
            return this.requestQueue.terminate(failure);
        }

    }

    /**
     * Admission, outbound request production, and wire release for one exchange.
     * <p>Outbound emission is serialized with wire release through {@link #gate}: cancelling the outbound subscriber
     * cannot stop a delivery that is already in flight, so {@link #terminate()} closes the gate instead of trusting
     * the cancel alone.
     */
    private static class ExchangeLifecycle extends AtomicBoolean implements Sinkable {

        private static final AtomicReferenceFieldUpdater<ExchangeLifecycle, RequestQueue.Lease> leaseUpdater = AtomicReferenceFieldUpdater.newUpdater(ExchangeLifecycle.class, RequestQueue.Lease.class, "lease");

        private static final AtomicIntegerFieldUpdater<ExchangeLifecycle> gateUpdater = AtomicIntegerFieldUpdater.newUpdater(ExchangeLifecycle.class, "gate");

        private static final int GATE_READY = 0;

        private static final int GATE_EMITTING = 1;

        private static final int GATE_TERMINATED = 2;

        private final ReactorNettyClient client;

        private final RequestSink requestSink;

        private final ConnectionContext context;

        private final Publisher<? extends ClientMessage> requests;

        private final Conversation conversation;

        private final Sinks.One<Flux<Message>> admission = Sinks.one();

        private volatile RequestQueue.Lease lease;

        private volatile int gate = GATE_READY;

        private final Disposable.Swap outbound = Disposables.swap();

        private ExchangeLifecycle(ReactorNettyClient client, Publisher<? extends ClientMessage> requests, Conversation conversation) {
            this.client = client;
            this.requestSink = client.requestSink;
            this.context = client.context;
            this.requests = requests;
            this.conversation = conversation;
        }

        private Flux<Message> exchange() {

            Flux<Message> response = Mono.defer(() -> {

                if (DEBUG_ENABLED) {
                    logger.debug(this.context.getMessage("exchange(subscribed)"));
                }

                if (!this.client.isConnected()) {
                    return Mono.error(CLOSED.get());
                }

                try {
                    this.requestSink.submit(this);
                } catch (Exception e) {
                    return Mono.error(e);
                }

                return this.admission.asMono();
            }).flatMapMany(Function.identity());

            return this.conversation.attach(response, this::terminate);
        }

        @Override
        public void admit(RequestQueue.Lease granted) {

            this.requestSink.incrementOutstandingRequests();
            leaseUpdater.set(this, granted);

            // The subscriber can be gone before the exchange is admitted, in which case the conversation has already
            // run its termination callback and nobody else would give this window back.
            if (this.conversation.isTerminated()) {
                terminate();
                return;
            }

            Flux<Message> response = this.client.responseProcessor.asFlux()
                    .doOnSubscribe(ignore -> startOutbound());

            this.admission.emitValue(response, Sinks.EmitFailureHandler.FAIL_FAST);
        }

        private void startOutbound() {

            if (!compareAndSet(false, true)) {
                // the conversation ended before this exchange got to the wire
                return;
            }

            Flux.from(this.requests).subscribe(new BaseSubscriber<ClientMessage>() {

                @Override
                protected void hookOnSubscribe(Subscription subscription) {

                    // Register before requesting so terminate() can cancel even a synchronous request publisher.
                    if (ExchangeLifecycle.this.outbound.update(this)) {
                        requestUnbounded();
                    }
                }

                @Override
                protected void hookOnNext(ClientMessage message) {

                    if (!gateUpdater.compareAndSet(ExchangeLifecycle.this, GATE_READY, GATE_EMITTING)) {
                        return;
                    }

                    try {
                        if (ExchangeLifecycle.this.client.isConnected()) {
                            ExchangeLifecycle.this.requestSink.emitNext(message);
                        }
                    } finally {
                        leaveGate();
                    }
                }

                @Override
                protected void hookOnError(Throwable throwable) {

                    // A late error belongs to an exchange that is already over; it must not end the shared sink.
                    if (!gateUpdater.compareAndSet(ExchangeLifecycle.this, GATE_READY, GATE_EMITTING)) {
                        Operators.onErrorDropped(throwable, currentContext());
                        return;
                    }

                    try {
                        ExchangeLifecycle.this.requestSink.emitError(throwable);
                    } finally {
                        leaveGate();
                    }
                }
            });
        }

        private void leaveGate() {
            if (!gateUpdater.compareAndSet(this, GATE_EMITTING, GATE_READY)) {
                finishTermination(true);
            }
        }

        @Override
        public void fail(Throwable throwable) {
            this.admission.emitError(throwable, Sinks.EmitFailureHandler.FAIL_FAST);
        }

        private void terminate() {

            this.outbound.dispose();

            int gate;
            do {
                gate = this.gate;

                if (gate == GATE_TERMINATED) {
                    break;
                }
            } while (!gateUpdater.compareAndSet(this, gate, GATE_TERMINATED));

            if (gate == GATE_EMITTING) {
                return;
            }

            finishTermination(false);
        }

        private void finishTermination(boolean deliveryCrossedTermination) {

            RequestQueue.Lease granted = leaseUpdater.getAndSet(this, null);

            if (granted == null) {
                return;
            }

            this.requestSink.decrementOutstandingRequests();

            // Claiming the wire here means this exchange never got to it: nothing was written, so the window is clean
            // no matter how the conversation ended. Losing the claim means outbound production started and the response
            // is ours.
            boolean reachedWire = !compareAndSet(false, true);

            if (reachedWire && (this.conversation.isAbandoned() || deliveryCrossedTermination)) {

                logger.error(this.client.context.getMessage(deliveryCrossedTermination
                        ? "An outbound message crossed the end of its conversation. Closing the connection because the response it provokes cannot be told apart from the next conversation."
                        : "Conversation abandoned before its final response frame. Closing the connection because the remaining response cannot be discarded."));

                this.client.close().subscribe(ignore -> {
                }, e -> logger.debug(this.client.context.getMessage("Failed to close connection of an abandoned conversation"), e));

                return;
            }

            granted.release();
        }

    }

    /**
     * Request queue to collect incoming exchange requests.
     * <p>Submission, release, and termination serialize access to the queue and its active-owner flag so a concurrent
     * submission is admitted, queued, or failed exactly once.
     */
    static class RequestQueue {

        // Access to requestQueue, active, and terminalFailure is guarded by this.
        private final Queue<Sinkable> requestQueue;

        private boolean active;

        @Nullable
        private Throwable terminalFailure;

        private final ConnectionContext context;

        RequestQueue(ConnectionContext context) {
            this(context, new ArrayBlockingQueue<>(Queues.SMALL_BUFFER_SIZE));
        }

        RequestQueue(ConnectionContext context, Queue<Sinkable> requestQueue) {
            this.context = context;
            this.requestQueue = requestQueue;
        }

        private void advance() {

            Sinkable nextCommand;

            synchronized (this) {

                if (this.terminalFailure != null) {
                    this.active = false;
                    return;
                }

                nextCommand = this.requestQueue.poll();

                if (nextCommand == null) {
                    this.active = false;
                }
            }

            if (nextCommand != null) {

                if (DEBUG_ENABLED) {
                    logger.debug(this.context.getMessage("Initiating queued exchange"));
                }

                // The callback can release its lease synchronously, so invoke it outside the monitor.
                nextCommand.admit(new Lease(this));
                return;
            }

            if (DEBUG_ENABLED) {
                logger.debug(this.context.getMessage("Conversation complete"));
            }
        }

        /**
         * Submit a {@code exchangeRequest}. Requests are either executed directly (without an active exchange) or queued (if another exchange is currently active).
         */
        void submit(Sinkable exchangeRequest) {

            boolean admitted = false;
            Throwable failure;

            synchronized (this) {
                failure = this.terminalFailure;

                if (failure == null) {
                    admitted = !this.active;

                    if (admitted) {
                        this.active = true;
                    } else if (!this.requestQueue.offer(exchangeRequest)) {
                        throw new IllegalStateException("Request queue is full");
                    }
                }
            }

            if (failure != null) {
                exchangeRequest.fail(failure);
            } else if (admitted) {

                if (DEBUG_ENABLED) {
                    logger.debug(this.context.getMessage("Initiating exchange"));
                }

                // The callback can release its lease synchronously, so invoke it outside the monitor.
                exchangeRequest.admit(new Lease(this));
            } else {

                if (DEBUG_ENABLED) {
                    logger.debug(this.context.getMessage("Queueing exchange"));
                }
            }
        }

        /**
         * Permanently terminate this queue and fail all pending and subsequent exchange requests. The first failure wins.
         *
         * @param failure the connection failure.
         * @return {@code true} if this call terminated the queue.
         */
        boolean terminate(Throwable failure) {

            Assert.requireNonNull(failure, "Failure must not be null");

            List<Sinkable> pending;

            synchronized (this) {

                if (this.terminalFailure != null) {
                    return false;
                }

                this.terminalFailure = failure;

                pending = new ArrayList<>();
                Sinkable request;
                while ((request = this.requestQueue.poll()) != null) {
                    pending.add(request);
                }
            }

            pending.forEach(request -> request.fail(failure));
            return true;
        }

        /**
         * Exclusive, single-use right to occupy the connection's request/response window. Issued to an exchange when it
         * is admitted and released once its conversation ends. Only the holder can advance the queue, so an exchange
         * that was never admitted cannot hand the wire to the next one while the current conversation is still running.
         */
        static final class Lease {

            private static final AtomicIntegerFieldUpdater<Lease> RELEASED = AtomicIntegerFieldUpdater.newUpdater(Lease.class, "released");

            private final RequestQueue queue;

            // access via RELEASED
            private volatile int released = 0;

            private Lease(RequestQueue queue) {
                this.queue = queue;
            }

            /**
             * Give up the window and admit the next queued exchange. Idempotent.
             *
             * @return {@code true} if this call released the window.
             */
            boolean release() {

                if (!RELEASED.compareAndSet(this, 0, 1)) {
                    return false;
                }

                this.queue.advance();
                return true;
            }

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

        /**
         * Admitted to the wire, holding {@code lease} until the conversation ends.
         */
        void admit(RequestQueue.Lease lease);

        void fail(Throwable throwable);

    }

    static class MssqlConnectionClosedException extends R2dbcNonTransientResourceException {

        public MssqlConnectionClosedException(String reason) {
            super(reason);
        }

    }

    public static class MssqlConnectionException extends R2dbcNonTransientResourceException {

        public MssqlConnectionException(Throwable cause) {
            super(cause);
        }

        public MssqlConnectionException(String reason, Throwable cause) {
            super(reason, cause);
        }

    }

}
