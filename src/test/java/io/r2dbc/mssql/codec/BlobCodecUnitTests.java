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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import io.r2dbc.spi.Blob;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BlobCodec}.
 *
 * @author Mark Paluch
 */
class BlobCodecUnitTests {

    @Test
    void shouldEncodeNull() {

        Encoded encoded = BlobCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("40 1F FF FF");
        assertThat(encoded.getFormalType()).isEqualTo("varbinary(8000)");
    }

    @Test
    void shouldBeAbleToEncodeNull() {

        assertThat(BlobCodec.INSTANCE.canEncodeNull(Blob.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeBinary() {

        TypeInformation binary =
            builder().withServerType(SqlServerType.BINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        assertThat(BlobCodec.INSTANCE.canDecode(ColumnUtil.createColumn(binary), Blob.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeVarBinary() {

        TypeInformation varbinary =
            builder().withServerType(SqlServerType.VARBINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        assertThat(BlobCodec.INSTANCE.canDecode(ColumnUtil.createColumn(varbinary), Blob.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeVarBinaryMax() {

        TypeInformation binary =
            builder().withServerType(SqlServerType.VARBINARYMAX).withLengthStrategy(LengthStrategy.PARTLENTYPE).build();

        assertThat(BlobCodec.INSTANCE.canDecode(ColumnUtil.createColumn(binary), Blob.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeImage() {

        TypeInformation binary =
            builder().withServerType(SqlServerType.IMAGE).withLengthStrategy(LengthStrategy.PARTLENTYPE).build();

        assertThat(BlobCodec.INSTANCE.canDecode(ColumnUtil.createColumn(binary), Blob.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeVarbinary() {

        TypeInformation varbinary =
            builder().withServerType(SqlServerType.VARBINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        ByteBuf data = HexUtils.decodeToByteBuf("03 00 62 61 72");

        Blob blob = BlobCodec.INSTANCE.decode(data, ColumnUtil.createColumn(varbinary), Blob.class);

        StepVerifier.create(blob.stream()).expectNext(ByteBuffer.wrap("bar".getBytes())).verifyComplete();
    }

    @Test
    void shouldBeAbleToDecodePlpStream() {

        TypeInformation varbinary =
            builder().withServerType(SqlServerType.VARBINARY).withLengthStrategy(LengthStrategy.PARTLENTYPE).build();

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer(12 + 24);
        PlpLength.of(24).encode(buffer);

        Length.of(8).encode(buffer, varbinary);
        buffer.writeBytes("C1xxxxxx".getBytes());

        Length.of(8).encode(buffer, varbinary);
        buffer.writeBytes("C2yyyyyy".getBytes());

        Length.of(8).encode(buffer, varbinary);
        buffer.writeBytes("C3zzzzzz".getBytes());

        Blob blob = BlobCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(varbinary), Blob.class);

        StepVerifier.create(blob.stream())
            .expectNext(ByteBuffer.wrap("C1xxxxxx".getBytes()))
            .expectNext(ByteBuffer.wrap("C2yyyyyy".getBytes()))
            .expectNext(ByteBuffer.wrap("C3zzzzzz".getBytes()))
            .verifyComplete();
    }

    @Test
    void shouldReleaseConsumedBuffers() {

        TypeInformation varbinary =
            builder().withServerType(SqlServerType.VARBINARY).withLengthStrategy(LengthStrategy.PARTLENTYPE).build();

        PooledByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;

        ByteBuf buffer = alloc.buffer();
        PlpLength.of(8).encode(buffer);

        Length.of(8).encode(buffer, varbinary);
        buffer.writeBytes("C1xxxxxx".getBytes());

        Blob blob = BlobCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(varbinary), Blob.class);
        buffer.release();

        StepVerifier.create(blob.stream())
            .expectNextCount(1)
            .verifyComplete();

        assertThat(buffer.refCnt()).isZero();
    }

    @Test
    void shouldReleaseRemainingBuffersOnCancel() {

        TypeInformation varbinary =
            builder().withServerType(SqlServerType.VARBINARY).withLengthStrategy(LengthStrategy.PARTLENTYPE).build();

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer(12 + 24);
        PlpLength.of(24).encode(buffer);

        Length.of(8).encode(buffer, varbinary);
        buffer.writeBytes("C1xxxxxx".getBytes());

        Length.of(8).encode(buffer, varbinary);
        buffer.writeBytes("C2yyyyyy".getBytes());

        Length.of(8).encode(buffer, varbinary);
        buffer.writeBytes("C3zzzzzz".getBytes());

        Blob blob = BlobCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(varbinary), Blob.class);
        buffer.release();

        assertThat(buffer.refCnt()).isEqualTo(3);

        StepVerifier.create(blob.stream(), 0)
            .thenRequest(1)
            .expectNextCount(1)
            .thenCancel()
            .verify();

        assertThat(buffer.refCnt()).isZero();
    }

    @Test
    void shouldReleaseOnDiscard() {

        TypeInformation varbinary =
            builder().withServerType(SqlServerType.VARBINARY).withLengthStrategy(LengthStrategy.PARTLENTYPE).build();

        PooledByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;

        ByteBuf buffer = alloc.buffer();
        PlpLength.of(8).encode(buffer);

        Length.of(8).encode(buffer, varbinary);
        buffer.writeBytes("C1xxxxxx".getBytes());

        Blob blob = BlobCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(varbinary), Blob.class);
        buffer.release();

        StepVerifier.create(blob.discard())
            .verifyComplete();

        assertThat(buffer.refCnt()).isZero();
    }
}
