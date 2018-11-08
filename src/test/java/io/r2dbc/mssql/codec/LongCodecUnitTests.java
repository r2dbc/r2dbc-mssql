/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import static io.r2dbc.mssql.message.type.TypeInformation.LengthStrategy;
import static io.r2dbc.mssql.message.type.TypeInformation.SqlServerType;
import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LongCodec}.
 *
 * @author Mark Paluch
 */
class LongCodecUnitTests {

    @Test
    void shouldEncodeLong() {

        Encoded encoded = LongCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), 72057594037927937L);

        EncodedAssert.assertThat(encoded).isEqualToHex("08 08 01 00 00 00 00 0 00 0 01");
        assertThat(encoded.getFormalType()).isEqualTo("bigint");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = LongCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("08 00");
        assertThat(encoded.getFormalType()).isEqualTo("bigint");
    }
    
    @Test
    void shouldBeAbleToDecode() {

        TypeInformation tinyint =
            builder().withServerType(SqlServerType.TINYINT).build();

        TypeInformation varchar =
            builder().withServerType(SqlServerType.VARCHAR).build();

        assertThat(LongCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), Long.class)).isTrue();
        assertThat(LongCodec.INSTANCE.canDecode(ColumnUtil.createColumn(varchar), Long.class)).isFalse();
        assertThat(LongCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), String.class)).isFalse();
    }

    @Test
    void shouldDecodeFromLong() {

        TypeInformation type = createType(8, SqlServerType.BIGINT);

        ByteBuf buffer = HexUtils.decodeToByteBuf("0100000000000001");

        assertThat(LongCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Long.class)).isEqualTo(72057594037927937L);
    }

    @Test
    void shouldDecodeFromInteger() {

        TypeInformation type = createType(4, SqlServerType.INTEGER);

        ByteBuf buffer = HexUtils.decodeToByteBuf("01000000");

        assertThat(LongCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Long.class)).isEqualTo(1);
    }

    @Test
    void shouldDecodeFromSmallInt() {

        TypeInformation type = createType(2, SqlServerType.SMALLINT);

        ByteBuf buffer = HexUtils.decodeToByteBuf("0100");

        assertThat(LongCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Long.class)).isEqualTo(1);
    }

    @Test
    void shouldDecodeTinyInt() {

        TypeInformation type = createType(1, SqlServerType.TINYINT);

        ByteBuf buffer = HexUtils.decodeToByteBuf("01");

        assertThat(LongCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Long.class)).isEqualTo(1);
    }

    private TypeInformation createType(int length, SqlServerType serverType) {
        return builder().withMaxLength(length).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withPrecision(length).withServerType(serverType).build();
    }
}
