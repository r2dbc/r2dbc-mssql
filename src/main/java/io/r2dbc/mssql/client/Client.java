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
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.ClientMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;

import org.reactivestreams.Publisher;

/**
 * An abstraction that wraps the networking part of exchanging methods.
 */
public interface Client {

	/**
	 * Release any resources held by the {@link Client}.
	 *
	 * @return a {@link Mono} that indicates that a client has been closed
	 */
	Mono<Void> close();

	/**
	 * Perform an exchange of messages.
	 *
	 * @param requests the publisher of outbound messages
	 * @return a {@link Flux} of incoming messages that ends with the end of the frame.
	 */
	Flux<Message> exchange(Publisher<ClientMessage> requests);

	/**
	 * Returns the {@link ByteBufAllocator}.
	 *
	 * @return the {@link ByteBufAllocator}
	 */
	ByteBufAllocator getByteBufAllocator();

	/**
	 * Returns a snapshot of the current parameter statuses.
	 *
	 * @return a snapshot of the current parameter statuses
	 */
	Map<String, String> getParameterStatus();

	/**
	 * Returns the connected process id if it has been communicated.
	 *
	 * @return the connected process id if it has been communicated
	 */
	Optional<Integer> getProcessId();

}
