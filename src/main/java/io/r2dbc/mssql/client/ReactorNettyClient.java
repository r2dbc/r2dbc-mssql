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

import static io.r2dbc.mssql.util.PredicateUtils.not;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.mssql.client.ssl.TdsSslHandler;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.PacketIdProvider;
import io.r2dbc.mssql.message.token.InfoToken;
import io.r2dbc.mssql.message.token.StreamDecoder;
import io.r2dbc.mssql.message.token.Tabular;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a TDS client based on the Reactor Netty project.
 *
 * @see TcpClient
 */
public final class ReactorNettyClient implements Client {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final AtomicReference<ByteBufAllocator> byteBufAllocator = new AtomicReference<>();

	private final AtomicReference<Connection> connection = new AtomicReference<>();
	/*
	private final BiConsumer<Message, SynchronousSink<Message>> handleErrorResponse = handleBackendMessage(ErrorResponse.class,
	    (message, sink) -> {
	        this.logger.error("Error: {}", toString(message.getFields()));
	        sink.next(message);
	    });
	
	private final BiConsumer<Message, SynchronousSink<Message>> handleNoticeResponse = handleBackendMessage(NoticeResponse.class,
	    (message, sink) -> this.logger.warn("Notice: {}", toString(message.getFields())));
	              */

	private final BiConsumer<Message, SynchronousSink<Message>> handleInfoToken = handleBackendMessage(Tabular.class,
			(message, sink) -> {

				List<InfoToken> tokens = message.getTokens(InfoToken.class);

				for (InfoToken token : tokens) {

					if (token.getInfoClass() < 9) {
						this.logger.debug("Info: Code {} Severity {}: {}", token.getNumber(), token.getInfoClass(),
								token.getMessage());
					}
				}
			});

	private final BiConsumer<Message, SynchronousSink<Message>> handleStateChange = handleBackendMessage(Message.class,
			(message, sink) -> {

				ConnectionState connectionState = this.state.get();

				if (connectionState.canAdvance(message)) {
					ConnectionState nextState = connectionState.next(message, this.connection.get());
					if (!this.state.compareAndSet(connectionState, nextState)) {
						sink.error(new ProtocolException(String.format("Cannot advance state from %s", connectionState)));
					}

					if (connectionState == ConnectionState.LOGIN && nextState == ConnectionState.POST_LOGIN && message instanceof Tabular) {
						sink.next(new ReadyForQuery((Tabular) message));
						return;
					}
				}

				sink.next(message);
			});

	private final AtomicBoolean isClosed = new AtomicBoolean(false);

	private final ConcurrentMap<String, String> parameterStatus = new ConcurrentHashMap<>();
	/*
	private final BiConsumer<Message, SynchronousSink<Message>> handleParameterStatus = handleBackendMessage(ParameterStatus.class,
	   (message, sink) -> this.parameterStatus.put(message.getName(), message.getValue()));
	  */
	private final AtomicReference<Integer> processId = new AtomicReference<>();

	private final EmitterProcessor<ClientMessage> requestProcessor = EmitterProcessor.create(false);

	private final FluxSink<ClientMessage> requests = this.requestProcessor.sink();

	private final EmitterProcessor<Flux<Message>> responseProcessor = EmitterProcessor.create(false);

	private final AtomicReference<ConnectionState> state = new AtomicReference<>(ConnectionState.PRELOGIN);

	private final PacketIdProvider packetIdProvider;

	/**
	 * Creates a new frame processor connected to a given TCP connection.
	 *
	 * @param connection the TCP connection
	 */
	private ReactorNettyClient(Connection connection, PacketIdProvider packetIdProvider) {
		Objects.requireNonNull(connection, "Connection must not be null");
		Objects.requireNonNull(packetIdProvider, "PacketIdProvider must not be null");

		FluxSink<Flux<Message>> responses = this.responseProcessor.sink();

		StreamDecoder decoder = new StreamDecoder();

		this.byteBufAllocator.set(connection.outbound().alloc());
		this.connection.set(connection);
		this.packetIdProvider = packetIdProvider;

		connection.inbound().receiveObject() //
				.concatMap(it -> {

					if (it instanceof ByteBuf) {

						ByteBuf buffer = (ByteBuf) it;
						buffer.retain();
						return decoder.decode(buffer, this.state.get().decoder());
					}

					if (it instanceof Message) {
						return Mono.just((Message) it);
					}

					return Mono.error(new ProtocolException(String.format("Unexpected protocol message: %s", it)));
				}) //
				.doOnNext(message -> this.logger.debug("Response: {}", message)) //
				.doOnError(message -> this.logger.warn("Error: {}", message)) //
				.handle(this.handleStateChange).handle(this.handleInfoToken)
				/*.handle(this.handleNoticeResponse)
				.handle(this.handleErrorResponse) */
				// .handle(this.handleBackendKeyData)
				// .handle(this.handleParameterStatus)
				// .handle(this.handleReadyForQuery)
				.doOnError(ProtocolException.class, e -> {
					this.isClosed.set(true);
					connection.channel().close();
				}).windowWhile(not(ReadyForQuery.class::isInstance)) //
				.subscribe(responses::next, responses::error, responses::complete);

		this.requestProcessor.doOnError(message -> {
			this.logger.warn("Error: {}", message);
			this.isClosed.set(true);
			connection.channel().close();
		}).doOnNext(message -> this.logger.debug("Request:  {}", message))
				.concatMap(
						message -> connection.outbound().send(message.encode(connection.outbound().alloc(), packetIdProvider)))
				.subscribe();
	}

	/**
	 * Creates a new frame processor connected to a given host.
	 *
	 * @param host the host to connect to
	 * @param port the port to connect to
	 */
	public static Mono<ReactorNettyClient> connect(String host, int port) {

		Objects.requireNonNull(host, "host must not be null");

		return connect(ConnectionProvider.newConnection(), host, port);
	}

	/**
	 * Creates a new frame processor connected to a given host.
	 *
	 * @param connectionProvider the connection provider resources
	 * @param host the host to connect to
	 * @param port the port to connect to
	 */
	public static Mono<ReactorNettyClient> connect(ConnectionProvider connectionProvider, String host, int port) {

		Objects.requireNonNull(connectionProvider, "connectionProvider must not be null");
		Objects.requireNonNull(host, "host must not be null");

		PacketIdProvider packetIdProvider = PacketIdProvider.atomic();

		Mono<? extends Connection> connection = TcpClient.create(connectionProvider).host(host).port(port).connect()
				.doOnNext(it -> {

					ChannelPipeline pipeline = it.channel().pipeline();
					InternalLogger logger = InternalLoggerFactory.getInstance(ReactorNettyClient.class);

					TdsSslHandler handler = new TdsSslHandler(packetIdProvider);
					if (logger.isDebugEnabled()) {

						pipeline.addFirst(LoggingHandler.class.getSimpleName(),
								new LoggingHandler(ReactorNettyClient.class, LogLevel.DEBUG));

						pipeline.addAfter(LoggingHandler.class.getSimpleName(), handler.getClass().getName(), handler);
					} else {
						pipeline.addFirst(handler.getClass().getName(), handler);
					}
				});

		return connection.map(it -> new ReactorNettyClient(it, packetIdProvider));
	}

	@Override
	public Mono<Void> close() {
		return Mono.defer(() -> {
			Connection connection = this.connection.getAndSet(null);

			if (connection == null) {
				return Mono.empty();
			}

			return Mono.fromRunnable(connection::dispose);
		});
	}

	@Override
	public Flux<Message> exchange(Publisher<ClientMessage> requests) {
		Objects.requireNonNull(requests, "requests must not be null");

		return Flux.defer(() -> {
			if (this.isClosed.get()) {
				return Flux.error(new IllegalStateException("Cannot exchange messages because the connection is closed"));
			}

			return this.responseProcessor
					.doOnSubscribe(s -> Flux.from(requests).subscribe(this.requests::next, this.requests::error)).next()
					.flatMapMany(Function.identity());
		});
	}

	@Override
	public ByteBufAllocator getByteBufAllocator() {
		return this.byteBufAllocator.get();
	}

	@Override
	public Map<String, String> getParameterStatus() {
		return new HashMap<>(this.parameterStatus);
	}

	@Override
	public Optional<Integer> getProcessId() {
		return Optional.ofNullable(this.processId.get());
	}

	@SuppressWarnings("unchecked")
	private static <T extends Message> BiConsumer<Message, SynchronousSink<Message>> handleBackendMessage(Class<T> type,
			BiConsumer<T, SynchronousSink<Message>> consumer) {
		return (message, sink) -> {
			if (type.isInstance(message)) {
				consumer.accept((T) message, sink);
			} else {
				sink.next(message);
			}
		};
	}
}
