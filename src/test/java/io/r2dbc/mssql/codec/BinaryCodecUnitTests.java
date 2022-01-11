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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BinaryCodec}.
 *
 * @author Mark Paluch
 */
class BinaryCodecUnitTests {

    @Test
    void shouldEncodeBinaryArray() {

        Encoded encoded = BinaryCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), "bar".getBytes());

        EncodedAssert.assertThat(encoded).isEqualToHex("40 1F 03 00 62 61 72");
        assertThat(encoded.getFormalType()).isEqualTo("varbinary(8000)");
    }

    @Test
    void shouldEncodeBinaryByteBuffer() {

        Encoded encoded = BinaryCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), ByteBuffer.wrap("bar".getBytes()));

        EncodedAssert.assertThat(encoded).isEqualToHex("40 1F 03 00 62 61 72");
        assertThat(encoded.getFormalType()).isEqualTo("varbinary(8000)");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = BinaryCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("40 1F FF FF");
        assertThat(encoded.getFormalType()).isEqualTo("varbinary(8000)");
    }

    @Test
    void shouldBeAbleToDecodeByteArray() {

        TypeInformation binary =
            builder().withServerType(SqlServerType.BINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        assertThat(BinaryCodec.INSTANCE.canDecode(ColumnUtil.createColumn(binary), byte[].class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeByteBuffer() {

        TypeInformation binary =
            builder().withServerType(SqlServerType.BINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        assertThat(BinaryCodec.INSTANCE.canDecode(ColumnUtil.createColumn(binary), ByteBuffer.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeVarbinaryToByteArray() {

        TypeInformation varbinary =
            builder().withServerType(SqlServerType.VARBINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        assertThat(BinaryCodec.INSTANCE.canDecode(ColumnUtil.createColumn(varbinary), byte[].class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeVarbinaryToByteBuffer() {

        TypeInformation varbinary =
            builder().withServerType(SqlServerType.VARBINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        assertThat(BinaryCodec.INSTANCE.canDecode(ColumnUtil.createColumn(varbinary), ByteBuffer.class)).isTrue();
    }

    @Test
    void shouldDecodeBinaryToByteArray() {

        TypeInformation binary =
            builder().withServerType(SqlServerType.BINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        ByteBuf data = HexUtils.decodeToByteBuf("0A 00 66 6F 6F 00 00 00 00 00 00 00");
        byte[] expected = new byte[10];
        expected[0] = 'f';
        expected[1] = 'o';
        expected[2] = 'o';

        assertThat(BinaryCodec.INSTANCE.decode(data, ColumnUtil.createColumn(binary), byte[].class)).isEqualTo(expected);
    }

    @Test
    void shouldDecodeBinaryToByteBuffer() {

        TypeInformation binary =
            builder().withServerType(SqlServerType.BINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        ByteBuf data = HexUtils.decodeToByteBuf("0A 00 66 6F 6F 00 00 00 00 00 00 00");
        ByteBuffer expected = ByteBuffer.allocate(10);
        expected.put("foo".getBytes()).put(new byte[7]).flip();

        assertThat(BinaryCodec.INSTANCE.decode(data, ColumnUtil.createColumn(binary), ByteBuffer.class)).isEqualTo(expected);
    }

    @Test
    void shouldDecodeBinaryNullToByteArray() {

        TypeInformation binary =
            builder().withServerType(SqlServerType.BINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        ByteBuf data = HexUtils.decodeToByteBuf("FF FF");

        assertThat(BinaryCodec.INSTANCE.decode(data, ColumnUtil.createColumn(binary), byte[].class)).isNull();
    }

    @Test
    void shouldDecodeBinaryNullToByteBuffer() {

        TypeInformation binary =
            builder().withServerType(SqlServerType.BINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        ByteBuf data = HexUtils.decodeToByteBuf("FF FF");

        assertThat(BinaryCodec.INSTANCE.decode(data, ColumnUtil.createColumn(binary), ByteBuffer.class)).isNull();
    }

    @Test
    void shouldDecodeVarBinaryToByteArray() {

        TypeInformation binary =
            builder().withServerType(SqlServerType.VARBINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        ByteBuf data = HexUtils.decodeToByteBuf("03 00 62 61 72");
        byte[] expected = "bar".getBytes();

        assertThat(BinaryCodec.INSTANCE.decode(data, ColumnUtil.createColumn(binary), byte[].class)).isEqualTo(expected);
    }

    @Test
    void shouldDecodeVarBinaryToByteBuffer() {

        TypeInformation binary =
            builder().withServerType(SqlServerType.VARBINARY).withLengthStrategy(LengthStrategy.USHORTLENTYPE).build();

        ByteBuf data = HexUtils.decodeToByteBuf("03 00 62 61 72");
        ByteBuffer expected = ByteBuffer.allocate(3);
        expected.put("bar".getBytes()).flip();

        assertThat(BinaryCodec.INSTANCE.decode(data, ColumnUtil.createColumn(binary), ByteBuffer.class)).isEqualTo(expected);
    }
}
