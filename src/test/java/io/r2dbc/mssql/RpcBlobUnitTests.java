/*
 * Copyright 2019-2021 the original author or authors.
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

package io.r2dbc.mssql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.r2dbc.mssql.client.TdsEncoder;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.codec.PlpEncoded;
import io.r2dbc.mssql.codec.RpcDirection;
import io.r2dbc.mssql.codec.RpcParameterContext;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.header.PacketIdProvider;
import io.r2dbc.mssql.message.token.RpcRequest;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.spi.Blob;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link Blob} encoding via {@link PlpEncoded} and {@link TdsEncoder}.
 *
 * @author Mark Paluch
 */
public class RpcBlobUnitTests {

    static byte[] ALL_BYTES = new byte[-(-128) + 127];

    static {
        for (int i = -128; i < 127; i++) {
            ALL_BYTES[-(-128) + i] = (byte) i;
        }
    }

    @Test
    void shouldEncodeChunkedStream() {

        int segmentsToGenerate = 3501;

        Blob blob = Blob.from(Flux.range(0, segmentsToGenerate).map(it -> ByteBuffer.wrap(ALL_BYTES)));

        DefaultCodecs codecs = new DefaultCodecs();

        Binding binding = new Binding();
        binding.add("P0", RpcDirection.IN, codecs.encode(ByteBufAllocator.DEFAULT, RpcParameterContext.in(), blob));

        RpcRequest request = RpcQueryMessageFlow.spExecuteSql("INSERT INTO lob_test values(@P0)", binding, Collation.RAW, TransactionDescriptor.empty());

        TdsEncoder encoder = new TdsEncoder(PacketIdProvider.just(1), 8000);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
        when(ctx.executor()).thenReturn(ImmediateEventExecutor.INSTANCE);
        ChannelPromise promise = mock(ChannelPromise.class);
        when(ctx.newPromise()).thenReturn(promise);
        when(ctx.write(any(), any(ChannelPromise.class))).then(invocationOnMock -> {

            ByteBuf buf = invocationOnMock.getArgument(0);

            int toRead = buf.readableBytes();
            byte[] bytes = new byte[toRead];
            buf.readBytes(bytes);

            if (buf != Unpooled.EMPTY_BUFFER) {
                buf.release();
            }

            return invocationOnMock.getArgument(1);
        });

        Flux.from(request.encode(ByteBufAllocator.DEFAULT, 8000))
            .publishOn(Schedulers.parallel())
            .doOnNext(it -> encoder.write(ctx, it, promise))
            .as(StepVerifier::create)
            .expectNextCount(32)
            .verifyComplete();
    }
}
