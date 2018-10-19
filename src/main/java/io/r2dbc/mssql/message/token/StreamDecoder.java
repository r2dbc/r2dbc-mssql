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
package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.Header;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * A decoder that reads {@link ByteBuf}s and returns a {@link Flux} of decoded {@link BackendMessage}s.
 */
public final class StreamDecoder {

	private final AtomicReference<ByteBuf> remainder = new AtomicReference<>();

	/**
	 * Decode a {@link ByteBuf} into a {@link Flux} of {@link Message}s. If the {@link ByteBuf} does not end on a
	 * {@link Message} boundary, the {@link ByteBuf} will be retained until an the concatenated contents of all retained
	 * {@link ByteBuf}s is a {@link Message} boundary.
	 *
	 * @param in the {@link ByteBuf} to decode
	 * @return a {@link Flux} of {@link Message}s
	 */
	public Flux<Message> decode(ByteBuf in, BiFunction<Header, ByteBuf, Message> decodeFunction) {
		Objects.requireNonNull(in, "in must not be null");

		return Flux.generate(() -> {
			ByteBuf remainder = this.remainder.getAndSet(null);
			return remainder == null ? in : Unpooled.wrappedBuffer(remainder, in);
		}, (byteBuf, sink) -> {

			if (byteBuf.readableBytes() < Header.SIZE) {

				this.remainder.set(byteBuf.retain());
				sink.complete();
				return byteBuf;
			}

			ByteBuf headerBytes = getHeader(byteBuf);

			if (!Header.canDecode(headerBytes)) {
				if (byteBuf.readableBytes() > 0) {
					this.remainder.set(byteBuf.retain());
				}

				sink.complete();
				return byteBuf;
			}

			try {

				Header header = Header.decode(headerBytes);
				ByteBuf body = getBody(header, byteBuf);

				Message message = decodeFunction.apply(header, body);

				if (message != null) {
					sink.next(message);
				}

			} catch (Exception e) {
				sink.error(e);
			}

			return byteBuf;
		}, ReferenceCountUtil::release);
	}

	static ByteBuf getHeader(ByteBuf in) {
		Objects.requireNonNull(in, "in must not be null");

		return in.readSlice(Header.SIZE);
	}

	@Nullable
	static ByteBuf getBody(Header header, ByteBuf in) {

		Objects.requireNonNull(in, "in must not be null");

		int bodyLength = header.getLength() - Header.SIZE;

		if (in.readableBytes() < bodyLength) {
			return null;
		}

		return in.readSlice(bodyLength);
	}
}
