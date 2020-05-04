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
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.PacketIdProvider;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.ContextualTdsFragment;
import io.r2dbc.mssql.message.tds.TdsFragment;
import io.r2dbc.mssql.message.tds.TdsPacket;
import io.r2dbc.mssql.message.tds.TdsPackets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.r2dbc.mssql.message.header.Status.StatusBit;
import static io.r2dbc.mssql.message.header.Status.empty;
import static io.r2dbc.mssql.message.header.Status.of;
import static io.r2dbc.mssql.util.EmbeddedChannelAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        channel.writeOutbound(HeaderOptions.create(Type.PRE_LOGIN, empty()));
        channel.writeOutbound(Unpooled.wrappedBuffer("foobar".getBytes()));

        assertThat(channel).outbound().hasByteBufMessage().isEmpty();
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.EOM, 0x0e, "foobar");
        });
    }

    @Test
    void shouldResetHeaderStatus() {

        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42)));

        channel.writeOutbound(HeaderOptions.create(Type.PRE_LOGIN, empty()));
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

        TdsPacket packet = TdsPackets.create(new Header(Type.PRE_LOGIN, of(StatusBit.EOM), 10, 0, 0, 0),
            Unpooled.wrappedBuffer("ab".getBytes()));

        channel.writeOutbound(packet);

        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.EOM, 0x0a, "ab");
        });
    }

    @Test
    void shouldEncodeContextualTdsFragment() {

        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42)));

        ContextualTdsFragment fragment = TdsPackets.create(HeaderOptions.create(Type.PRE_LOGIN, empty()),
            Unpooled.wrappedBuffer("ab".getBytes()));

        channel.writeOutbound(fragment);

        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

            encodeExpectation(buffer, StatusBit.EOM, 0x0a, "ab");
        });
    }

    @Test
    void shouldEncodeAndSplitContextualTdsFragment() {

        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42), 12));

        ContextualTdsFragment fragment = TdsPackets.create(HeaderOptions.create(Type.PRE_LOGIN, empty()),
            Unpooled.wrappedBuffer("foobar".getBytes()));

        channel.writeOutbound(fragment);

        // Chunk 1
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.NORMAL, 0x0c, "foob");
        });

        // Chunk 2
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.EOM, 0x0a, "ar");
        });
    }

    @Test
    void shouldEncodeTdsFragment() {

        EmbeddedChannel channel = new EmbeddedChannel();
        TdsEncoder tdsEncoder = new TdsEncoder(PacketIdProvider.just(42));
        tdsEncoder.setPacketSize(10);
        channel.pipeline().addFirst(tdsEncoder);

        TdsFragment fragment = TdsPackets.create(Unpooled.wrappedBuffer("ab".getBytes()));

        channel.writeOutbound(HeaderOptions.create(Type.PRE_LOGIN, empty()));
        channel.writeOutbound(fragment);

        assertThat(channel).outbound().hasByteBufMessage().isEmpty();
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.NORMAL, 0x0a, "ab");
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

        TdsEncoder encoder = new TdsEncoder(PacketIdProvider.just(42));
        encoder.setPacketSize(10);
        channel.pipeline().addFirst(encoder);

        TdsFragment first = TdsPackets.first(HeaderOptions.create(Type.PRE_LOGIN, empty()),
            Unpooled.wrappedBuffer("ab".getBytes()));

        TdsFragment last = TdsPackets.last(Unpooled.wrappedBuffer("ab".getBytes()));

        channel.writeOutbound(first, Unpooled.wrappedBuffer("fo".getBytes()), last,
            Unpooled.wrappedBuffer("fo".getBytes()));

        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.NORMAL, 0x0a, "ab");
        });

        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.EOM, 0x0a, "fo");
        });

        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.EOM, 0x0a, "ab");
        });

        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            buffer.writeBytes("fo".getBytes());
        });
    }

    @Test
    void shouldChunkMessagesLargeSmall() {

        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42), 12));

        TdsFragment first = TdsPackets.first(HeaderOptions.create(Type.PRE_LOGIN, empty()),
            Unpooled.wrappedBuffer("abcde".getBytes()));

        TdsFragment last = TdsPackets.last(Unpooled.wrappedBuffer("f".getBytes()));

        channel.writeOutbound(first, last);

        // Chunk 1
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.NORMAL, 0x0c, "abcd");
        });

        // Chunk 2
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.EOM, 0x0a, "ef");
        });
    }

    @Test
    void shouldChunkMessagesLargeLargeSmall() {

        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42), 12));

        TdsFragment first = TdsPackets.first(HeaderOptions.create(Type.PRE_LOGIN, empty()),
            Unpooled.wrappedBuffer("abcde".getBytes()));

        TdsFragment intermediate = TdsPackets.create(Unpooled.wrappedBuffer("fghijk".getBytes()));

        TdsFragment last = TdsPackets.last(Unpooled.wrappedBuffer("l".getBytes()));

        channel.writeOutbound(first, intermediate, last);

        // Chunk 1
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.NORMAL, 0x0c, "abcd");
        });

        // Chunk 2
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.NORMAL, 0x0c, "efgh");
        });

        // Chunk 3
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.EOM, 0x0c, "ijkl");
        });
    }

    @Test
    void shouldChunkMessagesLargeLargeLarge() {

        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addFirst(new TdsEncoder(PacketIdProvider.just(42), 12));

        TdsFragment first = TdsPackets.first(HeaderOptions.create(Type.PRE_LOGIN, empty()),
            Unpooled.wrappedBuffer("abcde".getBytes()));

        TdsFragment intermediate = TdsPackets.create(Unpooled.wrappedBuffer("fghijk".getBytes()));

        TdsFragment last = TdsPackets.last(Unpooled.wrappedBuffer("lmnop".getBytes()));

        channel.writeOutbound(first, intermediate, last);

        // Chunk 1
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {
            encodeExpectation(buffer, StatusBit.NORMAL, 0x0c, "abcd");
        });

        // Chunk 2
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

            encodeExpectation(buffer, StatusBit.NORMAL, 0x0c, "efgh");
        });

        // Chunk 3
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

            encodeExpectation(buffer, StatusBit.NORMAL, 0x0c, "ijkl");
        });

        // Chunk 4
        assertThat(channel).outbound().hasByteBufMessage().isEncodedAs(buffer -> {

            encodeExpectation(buffer, StatusBit.EOM, 0x0c, "mnop");
        });
    }

    @Test
    void shouldEstimateTdsPacketSize() {

        TdsEncoder encoder = new TdsEncoder(PacketIdProvider.just(42), 12);

        Assertions.assertThat(encoder.estimateChunkSize(1)).isEqualTo(9);
        Assertions.assertThat(encoder.estimateChunkSize(4)).isEqualTo(12);
        Assertions.assertThat(encoder.estimateChunkSize(5)).isEqualTo(12);
    }

    private static void encodeExpectation(ByteBuf buffer, StatusBit bit, int length, String content) {

        buffer.writeByte(18); // Type
        buffer.writeByte(bit.getBits()); // Status
        buffer.writeShort(length); // Length
        buffer.writeShort(0); // SPID
        buffer.writeByte(42); // PacketID
        buffer.writeByte(0); // Window
        buffer.writeBytes(content.getBytes());
    }
}
