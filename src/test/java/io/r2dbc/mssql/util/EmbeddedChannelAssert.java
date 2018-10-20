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
package io.r2dbc.mssql.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.embedded.EmbeddedChannel;

import java.nio.charset.Charset;
import java.util.Queue;
import java.util.function.Consumer;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;

public final class EmbeddedChannelAssert extends AbstractObjectAssert<EmbeddedChannelAssert, EmbeddedChannel> {

	private EmbeddedChannelAssert(EmbeddedChannel actual) {
		super(actual, EmbeddedChannelAssert.class);
	}

	public static EmbeddedChannelAssert assertThat(EmbeddedChannel actual) {
		return new EmbeddedChannelAssert(actual);
	}

	public MessagesAssert inbound() {
		return new MessagesAssert("inbound", this.actual.inboundMessages());
	}

	public MessagesAssert outbound() {
		return new MessagesAssert("outbound", this.actual.outboundMessages());
	}

	public static final class MessagesAssert extends AbstractObjectAssert<MessagesAssert, Queue<Object>> {

		private final String direction;

		private MessagesAssert(String direction, Queue<Object> actual) {
			super(actual, MessagesAssert.class);
			this.direction = direction;
		}

		public EncodedAssert hasByteBufMessage() {

			isNotNull();
			Object poll = actual.poll();

			Assertions.assertThat(poll).describedAs(this.direction + " message").isNotNull().isInstanceOf(ByteBuf.class);

			return new EncodedAssert((ByteBuf) poll);
		}
	}

	public static final class EncodedAssert extends AbstractObjectAssert<EncodedAssert, ByteBuf> {

		private EncodedAssert(ByteBuf actual) {
			super(actual, EncodedAssert.class);
		}

		public EncodedAssert contains(String expected) {
			return contains(expected, Charset.defaultCharset());
		}

		public EncodedAssert contains(String expected, Charset charset) {

			isNotNull();

			String actual = this.actual.toString(charset);

			Assertions.assertThat(actual).contains(expected);

			return this;
		}

		public void isEmpty() {
			Assertions.assertThat(this.actual.readableBytes()).isEqualTo(0);
		}

		public EncodedAssert isEncodedAs(Consumer<ByteBuf> encoded) {

			isNotNull();

			ByteBuf expected = TestByteBufAllocator.TEST.buffer();
			encoded.accept(expected);

			Assertions.assertThat(ByteBufUtil.prettyHexDump(actual)).describedAs("ByteBuf")
					.isEqualTo(ByteBufUtil.prettyHexDump(expected));

			return this;
		}
	}
}
