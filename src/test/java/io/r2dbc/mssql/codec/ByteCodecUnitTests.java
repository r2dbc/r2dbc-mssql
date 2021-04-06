/*
 * Copyright 2018-2021 the original author or authors.
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

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ByteCodec}.
 *
 * @author Mark Paluch
 */
class ByteCodecUnitTests {

    @Test
    void shouldEncodeByte() {

        Encoded encoded = ByteCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), (byte) 2);

        EncodedAssert.assertThat(encoded).isEqualToHex("01 01 02");
        assertThat(encoded.getFormalType()).isEqualTo("tinyint");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = ByteCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("01 00");
        assertThat(encoded.getFormalType()).isEqualTo("tinyint");
    }

    @Test
    void shouldBeAbleToDecode() {

        TypeInformation tinyint =
            builder().withServerType(SqlServerType.TINYINT).build();

        TypeInformation varchar =
            builder().withServerType(SqlServerType.VARCHAR).build();

        assertThat(ByteCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), Byte.class)).isTrue();
        assertThat(ByteCodec.INSTANCE.canDecode(ColumnUtil.createColumn(varchar), Byte.class)).isFalse();
        assertThat(ByteCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), String.class)).isFalse();
    }

    @Test
    void shouldDecodeFromLong() {

        TypeInformation type = createType(8, SqlServerType.BIGINT);

        ByteBuf buffer = HexUtils.decodeToByteBuf("FF00000000000000");

        assertThat(ByteCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Byte.class)).isEqualTo((byte) 255);
    }

    @Test
    void shouldDecodeFromInteger() {

        TypeInformation type = createType(4, SqlServerType.INTEGER);

        ByteBuf buffer = HexUtils.decodeToByteBuf("01000000");

        assertThat(ByteCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Byte.class)).isEqualTo((byte) 1);
    }

    @Test
    void shouldDecodeFromSmallInt() {

        TypeInformation type = createType(2, SqlServerType.SMALLINT);

        ByteBuf buffer = HexUtils.decodeToByteBuf("0100");

        assertThat(ByteCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Byte.class)).isEqualTo((byte) 1);
    }

    @Test
    void shouldDecodeTinyInt() {

        TypeInformation type = createType(1, SqlServerType.TINYINT);

        ByteBuf buffer = HexUtils.decodeToByteBuf("01");

        assertThat(ByteCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Byte.class)).isEqualTo((byte) 1);
    }

    private TypeInformation createType(int length, SqlServerType serverType) {
        return builder().withMaxLength(length).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withPrecision(length).withServerType(serverType).build();
    }
}
