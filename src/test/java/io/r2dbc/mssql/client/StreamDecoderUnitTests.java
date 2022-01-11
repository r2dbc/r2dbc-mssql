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
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.PacketIdProvider;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link StreamDecoder}.
 *
 * @author Mark Paluch
 */
class StreamDecoderUnitTests {

    static final Client CLIENT = TestClient.NO_OP;

    @Test
    void shouldDecodeFullPacket() {

        StreamDecoder decoder = new StreamDecoder();

        Header header = Header.create(HeaderOptions.create(Type.TABULAR_RESULT, Status.of(Status.StatusBit.EOM)), Header.LENGTH + DoneToken.LENGTH, PacketIdProvider.just(1));
        DoneToken token = DoneToken.create(2);

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();

        header.encode(buffer);
        token.encode(buffer);

        List<Message> messageStream = decoder.decode(buffer, ConnectionState.POST_LOGIN.decoder(CLIENT));

        assertThat(messageStream).containsOnly(token);

        assertThat(decoder.getDecoderState()).isNull();
        assertThat(buffer.refCnt()).isEqualTo(1);
        buffer.release();
    }

    @Test
    void shouldDecodePartialPacket() {

        StreamDecoder decoder = new StreamDecoder();

        DoneToken token = DoneToken.create(2);

        // Just the header type.
        ByteBuf partial = Unpooled.wrappedBuffer(new byte[]{4});

        List<Message> noMessage = decoder.decode(partial, ConnectionState.POST_LOGIN.decoder(CLIENT));
        assertThat(noMessage).isEmpty();

        StreamDecoder.DecoderState state = decoder.getDecoderState();
        assertThat(state).isNotNull();
        assertThat(state.header).isNull();
        assertThat(state.remainder.readableBytes()).isEqualTo(1);
        assertThat(state.aggregatedBody.readableBytes()).isEqualTo(0);
        assertThat(partial.refCnt()).isEqualTo(1);

        ByteBuf nextPacket = TestByteBufAllocator.TEST.buffer();
        nextPacket.writeBytes(new byte[]{1, 0, 0x15, 0, 0, 0, 0});
        token.encode(nextPacket);

        List<Message> completeMessage = decoder.decode(nextPacket, ConnectionState.POST_LOGIN.decoder(CLIENT));

        assertThat(completeMessage).containsOnly(token);
        assertThat(decoder.getDecoderState()).isNull();
        assertThat(partial.refCnt()).isEqualTo(1);
        assertThat(nextPacket.refCnt()).isEqualTo(1);

        partial.release();
        nextPacket.release();
    }

    @Test
    void shouldDecodePacketWithNextRemainder() {

        StreamDecoder decoder = new StreamDecoder();

        Header header = Header.create(HeaderOptions.create(Type.TABULAR_RESULT, Status.of(Status.StatusBit.EOM)), Header.LENGTH + DoneToken.LENGTH, PacketIdProvider.just(1));
        DoneToken token = DoneToken.create(2);

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();

        header.encode(buffer);
        token.encode(buffer);
        buffer.writeByte(4);

        List<Message> completeMessage = decoder.decode(buffer, ConnectionState.POST_LOGIN.decoder(CLIENT));

        assertThat(completeMessage).containsOnly(token);

        StreamDecoder.DecoderState state = decoder.getDecoderState();
        assertThat(state).isNotNull();
        assertThat(state.header).isNull();
        assertThat(state.remainder.readableBytes()).isEqualTo(1);
        assertThat(state.aggregatedBody.readableBytes()).isEqualTo(0);

        state.release();
        assertThat(buffer.refCnt()).isEqualTo(1);
        buffer.release();
    }

    @Test
    void shouldDecodePacketWithNextRemainderAfterNextHeader() {

        StreamDecoder decoder = new StreamDecoder();

        Header header = Header.create(HeaderOptions.create(Type.TABULAR_RESULT, Status.of(Status.StatusBit.EOM)), Header.LENGTH + DoneToken.LENGTH, PacketIdProvider.just(1));

        Header header2 = Header.create(HeaderOptions.create(Type.TABULAR_RESULT, Status.of(Status.StatusBit.EOM)), Header.LENGTH + DoneToken.LENGTH, PacketIdProvider.just(2));
        DoneToken token = DoneToken.create(2);

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();

        header.encode(buffer);
        token.encode(buffer);
        header2.encode(buffer);
        buffer.writeBytes(new byte[]{4, 2, 1});

        List<Message> completeMessage = decoder.decode(buffer, ConnectionState.POST_LOGIN.decoder(CLIENT));

        assertThat(completeMessage).containsOnly(token);

        StreamDecoder.DecoderState state = decoder.getDecoderState();
        assertThat(state).isNotNull();
        assertThat(state.header).isNotNull().isEqualTo(header2);
        assertThat(state.remainder.readableBytes()).isEqualTo(3);
        assertThat(state.aggregatedBody.readableBytes()).isEqualTo(0);

        buffer.release();
    }

    @Test
    void shouldDecodeTwoPacketsFragmented() {

        StreamDecoder decoder = new StreamDecoder();

        Header header = Header.create(HeaderOptions.create(Type.TABULAR_RESULT, Status.of(Status.StatusBit.EOM)), Header.LENGTH + DoneToken.LENGTH, PacketIdProvider.just(1));

        Header header2 = Header.create(HeaderOptions.create(Type.TABULAR_RESULT, Status.of(Status.StatusBit.EOM)), Header.LENGTH + DoneToken.LENGTH, PacketIdProvider.just(2));
        DoneToken token = DoneToken.create(2);

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();
        ByteBuf nextBuffer = TestByteBufAllocator.TEST.buffer();
        token.encode(nextBuffer);

        header.encode(buffer);
        token.encode(buffer);
        header2.encode(buffer);
        buffer.writeBytes(new byte[]{nextBuffer.readByte(), nextBuffer.readByte(), nextBuffer.readByte()});

        List<Message> firstMessage = decoder.decode(buffer, ConnectionState.POST_LOGIN.decoder(CLIENT));

        assertThat(firstMessage).containsOnly(token);

        StreamDecoder.DecoderState state = decoder.getDecoderState();
        assertThat(state).isNotNull();
        assertThat(state.header).isNotNull().isEqualTo(header2);
        assertThat(state.remainder.readableBytes()).isEqualTo(3);
        assertThat(state.aggregatedBody.readableBytes()).isEqualTo(0);
        assertThat(buffer.refCnt()).isEqualTo(1);

        List<Message> secondMessage = decoder.decode(nextBuffer, ConnectionState.POST_LOGIN.decoder(CLIENT));
        assertThat(secondMessage).containsOnly(token);

        assertThat(decoder.getDecoderState()).isNull();

        assertThat(buffer.refCnt()).isEqualTo(1);
        assertThat(nextBuffer.refCnt()).isEqualTo(1);

        buffer.release();
        nextBuffer.release();
    }

    @Test
    void shouldDecodeChunkedPackets() {

        StreamDecoder decoder = new StreamDecoder();

        Header firstHeader = Header.create(HeaderOptions.create(Type.TABULAR_RESULT, Status.empty()), Header.LENGTH + 3, PacketIdProvider.just(1));

        Header lastHeader = Header.create(HeaderOptions.create(Type.TABULAR_RESULT, Status.of(Status.StatusBit.EOM)), Header.LENGTH + 10, PacketIdProvider.just(2));
        DoneToken token = DoneToken.create(2);

        ByteBuf firstChunk = TestByteBufAllocator.TEST.buffer();
        ByteBuf lastChunk = TestByteBufAllocator.TEST.buffer();
        ByteBuf fullData = TestByteBufAllocator.TEST.buffer();
        token.encode(fullData);

        firstHeader.encode(firstChunk);
        firstChunk.writeBytes(new byte[]{fullData.readByte(), fullData.readByte(), fullData.readByte()});

        lastHeader.encode(lastChunk);
        lastChunk.writeBytes(fullData);

        List<Message> firstAttempt = decoder.decode(firstChunk, ConnectionState.POST_LOGIN.decoder(CLIENT));
        assertThat(firstAttempt).isEmpty();


        StreamDecoder.DecoderState state = decoder.getDecoderState();
        assertThat(state).isNotNull();
        assertThat(state.header).isNull(); // header completed
        assertThat(state.remainder.readableBytes()).isEqualTo(0);
        assertThat(state.aggregatedBody.readableBytes()).isEqualTo(3);
        assertThat(firstChunk.refCnt()).isEqualTo(1);

        List<Message> nextAttempt = decoder.decode(lastChunk, ConnectionState.POST_LOGIN.decoder(CLIENT));
        assertThat(nextAttempt).containsOnly(token);

        assertThat(decoder.getDecoderState()).isNull();

        assertThat(firstChunk.refCnt()).isEqualTo(1);
        assertThat(lastChunk.refCnt()).isEqualTo(1);

        firstChunk.release();
        lastChunk.release();
    }

    @Test
    void shouldDecodeManyChunks() {

        StreamDecoder decoder = new StreamDecoder();
        MessageDecoder messageDecoder = ConnectionState.POST_LOGIN.decoder(CLIENT);

        initializeColumMetadata(decoder, messageDecoder);

        List<ByteBuf> chunks = createChunks();

        // expect incomplete chunks to be empty
        for (int i = 0; i < chunks.size() - 1; i++) {
            assertThat(decoder.decode(chunks.get(i), messageDecoder)).isEmpty();
        }

        // Last chunk emits the data
        assertThat(decoder.decode(chunks.get(chunks.size() - 1), messageDecoder)).hasSize(1);

        StreamDecoder.DecoderState state = decoder.getDecoderState();
        assertThat(state).isNull();
        assertThat(decoder.getDecoderState()).isNull();
    }

    private List<ByteBuf> createChunks() {

        ByteBuf row = HexUtils.decodeToByteBuf("D1010C00700061006C007500630068" +
            "0004006D61726B080000000020A10700" +
            "10F17B0DC7C7E5C54098C7A12F7E6867" +
            "2408FED478E94628C6400437423146");

        List<ByteBuf> chunks = new ArrayList<>();

        while (row.isReadable()) {

            int bytesToRead = row.readableBytes();

            Status status = Status.empty();

            if (bytesToRead > 10) {
                bytesToRead = 10;
            } else {
                status = Status.of(Status.StatusBit.EOM);
            }

            Header header = Header.create(HeaderOptions.create(Type.TABULAR_RESULT, status), Header.LENGTH + bytesToRead, PacketIdProvider.just(1));
            ByteBuf chunk = TestByteBufAllocator.TEST.buffer();
            header.encode(chunk);
            chunk.writeBytes(row, bytesToRead);

            chunks.add(chunk);
        }
        return chunks;
    }

    private static void initializeColumMetadata(StreamDecoder decoder, MessageDecoder messageDecoder) {

        // Required initialization. ColMetadata does not support yet chunking.
        ByteBuf colmetadata = HexUtils.decodeToByteBuf("8107000000000000" +
            "000800300B65006D0070006C006F0079" +
            "00650065005F00690064000000000008" +
            "00E764000904D00034096C0061007300" +
            "74005F006E0061006D00650000000000" +
            "0900A732000904D000340A6600690072" +
            "00730074005F006E0061006D00650000" +
            "00000009006E0806730061006C006100" +
            "7200790000000000090024100366006F" +
            "006F000000000009006D080366006C00" +
            "74000000000009006D04036200610072" +
            "00");

        Header colHeader = Header.create(HeaderOptions.create(Type.TABULAR_RESULT, Status.of(Status.StatusBit.EOM)), Header.LENGTH + colmetadata.readableBytes(), PacketIdProvider.just(1));

        ByteBuf initialize = TestByteBufAllocator.TEST.heapBuffer();
        colHeader.encode(initialize);
        initialize.writeBytes(colmetadata);

        assertThat(decoder.decode(initialize, messageDecoder).get(0)).isInstanceOf(ColumnMetadataToken.class);
    }
}
