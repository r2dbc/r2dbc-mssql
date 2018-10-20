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
package io.r2dbc.mssql.client.tds;

import static io.r2dbc.mssql.util.EmbeddedChannelAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.PacketIdProvider;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TdsEncoder}.
 * 
 * @author Mark Paluch
 */
class TdsEncoderUnitTests {

	@Test
	void shouldPassThruByteBuffers() {

		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42)));

		channel.writeOutbound(Unpooled.wrappedBuffer("foobar".getBytes()));

		assertThat(channel).outbound().hasByteBufMessage().contains("foobar");
	}

	@Test
	void shouldPrependByteBuffersWithHeader() {

		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42)));

		channel.writeOutbound(HeaderOptions.create(Type.PRE_LOGIN, Status.empty()));
		channel.writeOutbound(Unpooled.wrappedBuffer("foobar".getBytes()));

		assertThat(channel).outbound().hasByteBufMessage().isEmpty();
		assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

			buffer.writeByte(18); // Type
			buffer.writeByte(0x01); // Status
			buffer.writeShort(0x0e); // Length
			buffer.writeShort(0); // SPID
			buffer.writeByte(42); // PacketID
			buffer.writeByte(0); // Window
			buffer.writeBytes("foobar".getBytes());
		});
	}

	@Test
	void shouldResetHeaderStatus() {

		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42)));

		channel.writeOutbound(HeaderOptions.create(Type.PRE_LOGIN, Status.empty()));
		channel.writeOutbound(TdsEncoder.ResetHeader.INSTANCE);
		channel.writeOutbound(Unpooled.wrappedBuffer("foobar".getBytes()));

		assertThat(channel).outbound().hasByteBufMessage().isEmpty();
		assertThat(channel).outbound().hasByteBufMessage().isEmpty();
		assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> buffer.writeBytes("foobar".getBytes()));
	}

	@Test
	void shouldEncodeTdsPacket() {

		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42)));

		TdsPacket packet = TdsPackets.create(new Header(Type.PRE_LOGIN, Status.of(Status.StatusBit.EOM), 10, 1, 0, 3),
				Unpooled.wrappedBuffer("ab".getBytes()));

		channel.writeOutbound(packet);

		assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

			buffer.writeByte(18); // Type
			buffer.writeByte(0x01); // Status
			buffer.writeShort(0x0a); // Length
			buffer.writeShort(1); // SPID
			buffer.writeByte(42); // PacketID
			buffer.writeByte(3); // Window
			buffer.writeBytes("ab".getBytes());
		});
	}

	@Test
	void shouldEncodeContextualTdsFragment() {

		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42)));

		ContextualTdsFragment fragment = TdsPackets.create(HeaderOptions.create(Type.PRE_LOGIN, Status.empty()),
				Unpooled.wrappedBuffer("ab".getBytes()));

		channel.writeOutbound(fragment);

		assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

			buffer.writeByte(18); // Type
			buffer.writeByte(0x01); // Status
			buffer.writeShort(0x0a); // Length
			buffer.writeShort(0); // SPID
			buffer.writeByte(42); // PacketID
			buffer.writeByte(0); // Window
			buffer.writeBytes("ab".getBytes());
		});
	}

	@Test
	void shouldEncodeTdsFragment() {

		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42)));

		TdsFragment fragment = TdsPackets.create(Unpooled.wrappedBuffer("ab".getBytes()));

		channel.writeOutbound(HeaderOptions.create(Type.PRE_LOGIN, Status.empty()));
		channel.writeOutbound(fragment);

		assertThat(channel).outbound().hasByteBufMessage().isEmpty();
		assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

			buffer.writeByte(18); // Type
			buffer.writeByte(0x00); // Status
			buffer.writeShort(0x0a); // Length
			buffer.writeShort(0); // SPID
			buffer.writeByte(42); // PacketID
			buffer.writeByte(0); // Window
			buffer.writeBytes("ab".getBytes());
		});
	}

	@Test
	void failsEncodingTdsFragmentWithoutHeader() {

		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42)));

		TdsFragment fragment = TdsPackets.create(Unpooled.wrappedBuffer("ab".getBytes()));

		assertThatThrownBy(() -> channel.writeOutbound(fragment)).isInstanceOf(IllegalStateException.class)
				.hasMessage("HeaderOptions must not be null!");
	}

	@Test
	void shouldEncodeMessageSequence() {

		EmbeddedChannel channel = new EmbeddedChannel();
		channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42)));

		TdsFragment first = TdsPackets.first(HeaderOptions.create(Type.PRE_LOGIN, Status.empty()),
				Unpooled.wrappedBuffer("ab".getBytes()));

		TdsFragment last = TdsPackets.last(Unpooled.wrappedBuffer("ab".getBytes()));

		channel.writeOutbound(first, Unpooled.wrappedBuffer("foo".getBytes()), last,
				Unpooled.wrappedBuffer("foo".getBytes()));

		assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

			buffer.writeByte(18); // Type
			buffer.writeByte(0x00); // Status
			buffer.writeShort(0x0a); // Length
			buffer.writeShort(0); // SPID
			buffer.writeByte(42); // PacketID
			buffer.writeByte(0); // Window
			buffer.writeBytes("ab".getBytes());
		});

		assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

			buffer.writeByte(18); // Type
			buffer.writeByte(0x01); // Status – shouldn't we reset the EOM state if there is a binary message between First
															// and Last TDS fragments?
			buffer.writeShort(0x0b); // Length
			buffer.writeShort(0); // SPID
			buffer.writeByte(42); // PacketID
			buffer.writeByte(0); // Window
			buffer.writeBytes("foo".getBytes());
		});

		assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

			buffer.writeByte(18); // Type
			buffer.writeByte(0x01); // Status
			buffer.writeShort(0x0a); // Length
			buffer.writeShort(0); // SPID
			buffer.writeByte(42); // PacketID
			buffer.writeByte(0); // Window
			buffer.writeBytes("ab".getBytes());
		});

		assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

			buffer.writeBytes("foo".getBytes());
		});
	}
}
