/*
 * Copyright 2019-2022 the original author or authors.
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

package io.r2dbc.mssql.client.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.r2dbc.mssql.MssqlConnectionConfiguration;
import io.r2dbc.mssql.client.ClientConfiguration;
import io.r2dbc.mssql.client.ConnectionContext;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.PacketIdProvider;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.security.GeneralSecurityException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TdsSslHandler}.
 *
 * @author Mark Paluch
 * @author Omer Kissos
 */
class TdsSslHandlerUnitTests {

     MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
                .host("foo").port(1234).enableSsl()
                .username("sa").password("sa")
                .build();

     ClientConfiguration clientConfiguration = this.configuration.toClientConfiguration();

    TdsSslHandler handler;

    SslHandler sslHandler = mock(SslHandler.class);

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);

    @BeforeEach
    void setUp() {

        this.handler = new TdsSslHandler(PacketIdProvider.just(0), this.clientConfiguration, new ConnectionContext());
        this.handler.setSslHandler(this.sslHandler);
        this.handler.setState(SslState.CONNECTION);
    }

    @Test
    void entireMessage() throws Exception {

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();

        Header header = new Header(Type.PRE_LOGIN, Status.of(Status.StatusBit.EOM), 100, 1);
        header.encode(buffer);

        IntStream.range(0, 92).forEach(buffer::writeByte);

        this.handler.channelRead(this.ctx, buffer);

        verify(this.sslHandler).channelRead(any(), this.captor.capture());

        ByteBuf value = this.captor.getValue();
        assertThat(value.readableBytes()).isEqualTo(92);
    }

    @Test
    void singleChunkedMessage() throws Exception {

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();
        ByteBuf expected = TestByteBufAllocator.TEST.buffer();

        Header header = new Header(Type.PRE_LOGIN, Status.of(Status.StatusBit.EOM), 100, 1);
        header.encode(buffer);

        IntStream.range(0, 92).forEach(buffer::writeByte);
        IntStream.range(0, 92).forEach(expected::writeByte);

        while (buffer.isReadable()) {
            this.handler.channelRead(this.ctx, buffer.readRetainedSlice(Math.min(10, buffer.readableBytes())));
        }
        buffer.release();

        verify(this.sslHandler).channelRead(any(), this.captor.capture());

        ByteBuf value = this.captor.getValue();
        assertThat(value.readableBytes()).isEqualTo(92);
        assertThat(value).isEqualTo(expected);
    }

    @Test
    void multipleChunkedMessages() throws Exception {

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();
        ByteBuf expected = TestByteBufAllocator.TEST.buffer();

        Header chunk1 = new Header(Type.PRE_LOGIN, Status.empty(), 100, 1);
        chunk1.encode(buffer);
        IntStream.range(0, 92).forEach(buffer::writeByte);

        Header chunk2 = new Header(Type.PRE_LOGIN, Status.of(Status.StatusBit.EOM), 50, 1);
        chunk2.encode(buffer);
        IntStream.range(0, 42).forEach(buffer::writeByte);

        IntStream.range(0, 92).forEach(expected::writeByte);
        IntStream.range(0, 42).forEach(expected::writeByte);

        this.handler.channelRead(this.ctx, buffer.readRetainedSlice(Math.min(80, buffer.readableBytes())));
        this.handler.channelRead(this.ctx, buffer.readRetainedSlice(Math.min(50, buffer.readableBytes())));
        this.handler.channelRead(this.ctx, buffer.readRetainedSlice(Math.min(20, buffer.readableBytes())));
        buffer.release();

        verify(this.sslHandler).channelRead(any(), this.captor.capture());

        ByteBuf value = this.captor.getValue();
        assertThat(value.readableBytes()).isEqualTo(92 + 42);
    }

    @Test
    void channelInactiveReleasesChunk() throws Exception {

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();
        Header header = new Header(Type.PRE_LOGIN, Status.of(Status.StatusBit.EOM), 100, 1);
        header.encode(buffer);

        IntStream.range(0, 20).forEach(buffer::writeByte);

        this.handler.channelRead(this.ctx, buffer.readRetainedSlice(Math.min(10, buffer.readableBytes())));
        this.handler.channelRead(this.ctx, buffer.readRetainedSlice(Math.min(10, buffer.readableBytes())));

        buffer.release();
        assertThat(buffer.refCnt()).isNotZero();

        this.handler.channelInactive(this.ctx);
        assertThat(buffer.refCnt()).isZero();
    }

    @Test
    void incompleteHandshakeBufferIsReleasedAfterDefragmentation() throws Exception {

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();
        Header header = new Header(Type.PRE_LOGIN, Status.empty(), 100, 1);
        header.encode(buffer);

        IntStream.range(0, 20).forEach(buffer::writeByte);

        ByteBuf retained = buffer.readRetainedSlice(buffer.readableBytes());
        buffer.release();

        assertThat(retained.refCnt()).isEqualTo(1);

        this.handler.channelRead(this.ctx, retained);

        assertThat(retained.refCnt()).isZero();
    }
}
