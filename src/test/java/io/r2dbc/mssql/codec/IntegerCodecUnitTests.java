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
 * Unit tests for {@link IntegerCodec}.
 *
 * @author Mark Paluch
 */
class IntegerCodecUnitTests {

    @Test
    void shouldEncodeInteger() {

        Encoded encoded = IntegerCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), 16777217);

        EncodedAssert.assertThat(encoded).isEqualToHex("04 04 01 00 00 01");
        assertThat(encoded.getFormalType()).isEqualTo("int");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = IntegerCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("04 00");
        assertThat(encoded.getFormalType()).isEqualTo("int");
    }

    @Test
    void shouldBeAbleToDecode() {

        TypeInformation tinyint =
            builder().withServerType(SqlServerType.TINYINT).build();

        TypeInformation varchar =
            builder().withServerType(SqlServerType.VARCHAR).build();

        TypeInformation numeric =
            builder().withServerType(SqlServerType.NUMERIC).build();

        assertThat(IntegerCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), Integer.class)).isTrue();
        assertThat(IntegerCodec.INSTANCE.canDecode(ColumnUtil.createColumn(varchar), Integer.class)).isFalse();
        assertThat(IntegerCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), String.class)).isFalse();
        assertThat(IntegerCodec.INSTANCE.canDecode(ColumnUtil.createColumn(numeric), Integer.class)).isTrue();
    }

    @Test
    void shouldDecodeFromLong() {

        TypeInformation type = createType(8, SqlServerType.BIGINT);

        ByteBuf buffer = HexUtils.decodeToByteBuf("0100000100000000");

        assertThat(IntegerCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Integer.class)).isEqualTo(16777217);
    }

    @Test
    void shouldDecodeFromInteger() {

        TypeInformation type = createType(4, SqlServerType.INTEGER);

        ByteBuf buffer = HexUtils.decodeToByteBuf("01000000");

        assertThat(IntegerCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Integer.class)).isEqualTo(1);
    }

    @Test
    void shouldDecodeFromNumeric() {

        TypeInformation type = TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.NUMERIC).withScale(0).withPrecision(5).build();

        ByteBuf buffer = HexUtils.decodeToByteBuf("05 01 39 30 00 00");

        assertThat(IntegerCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Integer.class)).isEqualTo(12345);
    }

    @Test
    void shouldDecodeFromSmallInt() {

        TypeInformation type = createType(2, SqlServerType.SMALLINT);

        ByteBuf buffer = HexUtils.decodeToByteBuf("0100");

        assertThat(IntegerCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Integer.class)).isEqualTo(1);
    }

    @Test
    void shouldDecodeTinyInt() {

        TypeInformation type = createType(1, SqlServerType.TINYINT);

        ByteBuf buffer = HexUtils.decodeToByteBuf("01");

        assertThat(IntegerCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Integer.class)).isEqualTo(1);
    }

    private TypeInformation createType(int length, SqlServerType serverType) {
        return builder().withMaxLength(length).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withPrecision(length).withServerType(serverType).build();
    }
}
