/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Unit tests for {@link PlpEncoded}.
 *
 * @author Mark Paluch
 */
class PlpEncodedUnitTests {

    @Test
    void shouldSplitBigByteArray() {

        byte[] bytes = new byte[255];

        for (int i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
            bytes[i - Byte.MIN_VALUE] = (byte) i;
        }

        PlpEncoded encoded = new PlpEncoded(SqlServerType.VARBINARYMAX, TestByteBufAllocator.TEST, Mono.just(Unpooled.wrappedBuffer(bytes)), () -> {

        });

        encoded.chunked(() -> 100, false).as(StepVerifier::create)
            .assertNext(actual -> assertThat(actual.readableBytes()).isEqualTo(100))
            .assertNext(actual -> assertThat(actual.readableBytes()).isEqualTo(100))
            .assertNext(actual -> assertThat(actual.readableBytes()).isEqualTo(55))
            .verifyComplete();
    }

    @Test
    void shouldSplitBigByteArrayWithPlpHeaders() {

        byte[] bytes = new byte[255];

        for (int i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
            bytes[i - Byte.MIN_VALUE] = (byte) i;
        }

        PlpEncoded encoded = new PlpEncoded(SqlServerType.VARBINARYMAX, TestByteBufAllocator.TEST, Mono.just(Unpooled.wrappedBuffer(bytes)), () -> {

        });

        encoded.chunked(() -> 100, true).as(StepVerifier::create)
            .assertNext(actual -> assertThat(actual.readableBytes()).isEqualTo(100 + 8 + 4))
            .assertNext(actual -> assertThat(actual.readableBytes()).isEqualTo(100 + 4))
            .assertNext(actual -> assertThat(actual.readableBytes()).isEqualTo(55 + 4))
            .verifyComplete();
    }

    @Test
    void shouldRearrangeChunks() {

        byte[] bytes1 = new byte[255];
        byte[] bytes2 = new byte[255];

        for (int i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
            bytes1[i - Byte.MIN_VALUE] = (byte) i;
            bytes2[i - Byte.MIN_VALUE] = (byte) i;
        }

        PlpEncoded encoded = new PlpEncoded(SqlServerType.VARBINARYMAX, TestByteBufAllocator.TEST, Flux.just(Unpooled.wrappedBuffer(bytes1), Unpooled.wrappedBuffer(bytes2)), () -> {
        });

        AtomicInteger size = new AtomicInteger(100);

        encoded.chunked(size::get).as(StepVerifier::create)
            .assertNext(actual -> {
                size.incrementAndGet();
                assertThat(actual.readableBytes()).isEqualTo(100);
            }).assertNext(actual -> {
            size.incrementAndGet();
            assertThat(actual.readableBytes()).isEqualTo(101);
        }).assertNext(actual -> {
            size.incrementAndGet();
            assertThat(actual.readableBytes()).isEqualTo(102);
        }).assertNext(actual -> {
            size.incrementAndGet();
            assertThat(actual.readableBytes()).isEqualTo(103);
        }).assertNext(actual -> {
            assertThat(actual.readableBytes()).isEqualTo(104);
        }).verifyComplete();
    }

    @Test
    void shouldDisposeUnusedBuffers() {

        AtomicBoolean dispose = new AtomicBoolean();

        PlpEncoded encoded = new PlpEncoded(SqlServerType.VARBINARYMAX, TestByteBufAllocator.TEST, Flux.empty(),
            () -> dispose.set(true));

        encoded.release();

        assertThat(dispose).isTrue();
    }

    @Test
    void emptySequenceShouldNeverAllocateCompositeBuffer() {

        AtomicBoolean dispose = new AtomicBoolean();
        ByteBufAllocator alloc = mock(ByteBufAllocator.class);
        CompositeByteBuf composite = spy(TestByteBufAllocator.TEST.compositeBuffer());
        doReturn(composite).when(alloc).compositeBuffer();

        PlpEncoded encoded = new PlpEncoded(SqlServerType.VARBINARYMAX, alloc, Flux.empty(),
            () -> dispose.set(true));

        StepVerifier.create(encoded.chunked(() -> 1), 0).thenRequest(1).verifyComplete();

        verifyZeroInteractions(alloc);
    }

    @Test
    void shouldDisposeCompositeBufferOnComplete() {

        AtomicBoolean dispose = new AtomicBoolean();
        ByteBufAllocator alloc = mock(ByteBufAllocator.class);
        CompositeByteBuf composite = spy(TestByteBufAllocator.TEST.compositeBuffer());
        doReturn(composite).when(alloc).compositeBuffer();

        PlpEncoded encoded = new PlpEncoded(SqlServerType.VARBINARYMAX, alloc, Flux.just(Unpooled.wrappedBuffer(new byte[]{1, 2, 3})),
            () -> dispose.set(true));

        encoded.chunked(() -> 1)
            .as(StepVerifier::create)
            .expectNextCount(3)
            .verifyComplete();

        verify(composite).release();
    }

    @Test
    void shouldDisposeCompositeBufferOnCancel() {

        AtomicBoolean dispose = new AtomicBoolean();
        ByteBufAllocator alloc = mock(ByteBufAllocator.class);
        CompositeByteBuf composite = spy(TestByteBufAllocator.TEST.compositeBuffer());
        doReturn(composite).when(alloc).compositeBuffer();

        PlpEncoded encoded = new PlpEncoded(SqlServerType.VARBINARYMAX, alloc, Flux.just(Unpooled.wrappedBuffer(new byte[]{1, 2, 3})),
            () -> dispose.set(true));

        StepVerifier.create(encoded.chunked(() -> 1), 0)
            .thenRequest(1)
            .expectNextCount(1)
            .thenCancel()
            .verify();

        verify(composite).release();
    }

    @Test
    void shouldDisposeCompositeBufferOnError() {

        AtomicBoolean dispose = new AtomicBoolean();
        ByteBufAllocator alloc = mock(ByteBufAllocator.class);
        CompositeByteBuf composite = spy(TestByteBufAllocator.TEST.compositeBuffer());
        doReturn(composite).when(alloc).compositeBuffer();

        PlpEncoded encoded = new PlpEncoded(SqlServerType.VARBINARYMAX, alloc, Flux.concat(Mono.just(Unpooled.wrappedBuffer(new byte[]{1, 2, 3})), Mono.error(new IllegalStateException())),
            () -> dispose.set(true));

        encoded.chunked(() -> 1).as(StepVerifier::create)
            .expectNextCount(3)
            .verifyError(IllegalStateException.class);

        verify(composite).release();
    }
}
