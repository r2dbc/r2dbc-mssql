/*
 * Copyright 2018 the original author or authors.
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
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

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

        Flux<Message> messageStream = decoder.decode(buffer, ConnectionState.POST_LOGIN.decoder(CLIENT));

        messageStream.as(StepVerifier::create)
            .expectNext(token)
            .verifyComplete();

        assertThat(decoder.getDecoderState()).isNull();
        assertThat(buffer.refCnt()).isEqualTo(0);
    }

    @Test
    void shouldDecodePartialPacket() {

        StreamDecoder decoder = new StreamDecoder();

        DoneToken token = DoneToken.create(2);

        // Just the header type.
        ByteBuf partial = Unpooled.wrappedBuffer(new byte[]{4});

        Flux<Message> noMessage = decoder.decode(partial, ConnectionState.POST_LOGIN.decoder(CLIENT));

        noMessage.as(StepVerifier::create)
            .verifyComplete();

        StreamDecoder.DecoderState state = decoder.getDecoderState();
        assertThat(state).isNotNull();
        assertThat(state.header).isNull();
        assertThat(state.remainder.readableBytes()).isEqualTo(1);
        assertThat(state.aggregatedBody.readableBytes()).isEqualTo(0);
        assertThat(partial.refCnt()).isEqualTo(1);

        ByteBuf nextPacket = TestByteBufAllocator.TEST.buffer();
        nextPacket.writeBytes(new byte[]{1, 0, 0x15, 0, 0, 0, 0});
        token.encode(nextPacket);

        Flux<Message> completeMessage = decoder.decode(nextPacket, ConnectionState.POST_LOGIN.decoder(CLIENT));

        completeMessage.as(StepVerifier::create)
            .expectNext(token)
            .verifyComplete();

        assertThat(decoder.getDecoderState()).isNull();
        assertThat(partial.refCnt()).isEqualTo(0);

        assertThat(nextPacket.refCnt()).isEqualTo(0);
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

        Flux<Message> completeMessage = decoder.decode(buffer, ConnectionState.POST_LOGIN.decoder(CLIENT));

        completeMessage.as(StepVerifier::create)
            .expectNext(token)
            .verifyComplete();

        StreamDecoder.DecoderState state = decoder.getDecoderState();
        assertThat(state).isNotNull();
        assertThat(state.header).isNull();
        assertThat(state.remainder.readableBytes()).isEqualTo(1);
        assertThat(state.aggregatedBody.readableBytes()).isEqualTo(0);

        assertThat(buffer.refCnt()).isEqualTo(1);
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

        Flux<Message> completeMessage = decoder.decode(buffer, ConnectionState.POST_LOGIN.decoder(CLIENT));

        completeMessage.as(StepVerifier::create)
            .expectNext(token)
            .verifyComplete();

        StreamDecoder.DecoderState state = decoder.getDecoderState();
        assertThat(state).isNotNull();
        assertThat(state.header).isNotNull().isEqualTo(header2);
        assertThat(state.remainder.readableBytes()).isEqualTo(3);
        assertThat(state.aggregatedBody.readableBytes()).isEqualTo(0);
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

        Flux<Message> firstMessage = decoder.decode(buffer, ConnectionState.POST_LOGIN.decoder(CLIENT));

        firstMessage.as(StepVerifier::create)
            .expectNext(token)
            .verifyComplete();

        StreamDecoder.DecoderState state = decoder.getDecoderState();
        assertThat(state).isNotNull();
        assertThat(state.header).isNotNull().isEqualTo(header2);
        assertThat(state.remainder.readableBytes()).isEqualTo(3);
        assertThat(state.aggregatedBody.readableBytes()).isEqualTo(0);
        assertThat(buffer.refCnt()).isEqualTo(1);

        Flux<Message> secondMessage = decoder.decode(nextBuffer, ConnectionState.POST_LOGIN.decoder(CLIENT));

        secondMessage.as(StepVerifier::create)
            .expectNext(token)
            .verifyComplete();

        assertThat(decoder.getDecoderState()).isNull();

        assertThat(buffer.refCnt()).isEqualTo(0);
        assertThat(nextBuffer.refCnt()).isEqualTo(0);
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

        Flux<Message> firstAttempt = decoder.decode(firstChunk, ConnectionState.POST_LOGIN.decoder(CLIENT));

        firstAttempt.as(StepVerifier::create)
            .verifyComplete();

        StreamDecoder.DecoderState state = decoder.getDecoderState();
        assertThat(state).isNotNull();
        assertThat(state.header).isNull(); // header completed
        assertThat(state.remainder.readableBytes()).isEqualTo(0);
        assertThat(state.aggregatedBody.readableBytes()).isEqualTo(3);
        assertThat(firstChunk.refCnt()).isEqualTo(1);

        Flux<Message> nextAttempt = decoder.decode(lastChunk, ConnectionState.POST_LOGIN.decoder(CLIENT));

        nextAttempt.as(StepVerifier::create)
            .expectNext(token)
            .verifyComplete();

        assertThat(decoder.getDecoderState()).isNull();

        assertThat(firstChunk.refCnt()).isEqualTo(0);
        assertThat(lastChunk.refCnt()).isEqualTo(0);
    }
}
