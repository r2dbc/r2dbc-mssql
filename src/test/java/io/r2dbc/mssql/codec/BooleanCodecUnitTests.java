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
 * Unit tests for {@link BooleanCodec}.
 *
 * @author Mark Paluch
 */
class BooleanCodecUnitTests {

    @Test
    void shouldEncodeBoolean() {

        Encoded encoded = BooleanCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), true);
        EncodedAssert.assertThat(encoded).isEqualToHex("01 01 01");
        assertThat(encoded.getFormalType()).isEqualTo("tinyint");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = BooleanCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("01 00");
        assertThat(encoded.getFormalType()).isEqualTo("tinyint");
    }

    @Test
    void shouldBeAbleToDecode() {

        TypeInformation tinyint =
            builder().withServerType(SqlServerType.TINYINT).build();

        TypeInformation varchar =
            builder().withServerType(SqlServerType.VARCHAR).build();

        assertThat(BooleanCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), Boolean.class)).isTrue();
        assertThat(BooleanCodec.INSTANCE.canDecode(ColumnUtil.createColumn(varchar), Boolean.class)).isFalse();
        assertThat(BooleanCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), String.class)).isFalse();
    }

    @Test
    void shouldDecodeFromLong() {

        TypeInformation type = createType(8, SqlServerType.BIGINT);

        assertThat(BooleanCodec.INSTANCE.decode(HexUtils.decodeToByteBuf("0000000000000001"), ColumnUtil.createColumn(type), Boolean.class)).isTrue();
        assertThat(BooleanCodec.INSTANCE.decode(HexUtils.decodeToByteBuf("0000000000000000"), ColumnUtil.createColumn(type), Boolean.class)).isFalse();
    }

    @Test
    void shouldDecodeFromInteger() {

        TypeInformation type = createType(4, SqlServerType.INTEGER);

        assertThat(BooleanCodec.INSTANCE.decode(HexUtils.decodeToByteBuf("01000000"), ColumnUtil.createColumn(type), Boolean.class)).isTrue();
        assertThat(BooleanCodec.INSTANCE.decode(HexUtils.decodeToByteBuf("00000000"), ColumnUtil.createColumn(type), Boolean.class)).isFalse();
    }

    @Test
    void shouldDecodeFromSmallInt() {

        TypeInformation type = createType(2, SqlServerType.SMALLINT);

        assertThat(BooleanCodec.INSTANCE.decode(HexUtils.decodeToByteBuf("0100"), ColumnUtil.createColumn(type), Boolean.class)).isTrue();
    }

    @Test
    void shouldDecodeFromTinyInt() {

        TypeInformation type = createType(1, SqlServerType.TINYINT);

        assertThat(BooleanCodec.INSTANCE.decode(HexUtils.decodeToByteBuf("01"), ColumnUtil.createColumn(type), Boolean.class)).isTrue();
        assertThat(BooleanCodec.INSTANCE.decode(HexUtils.decodeToByteBuf("00"), ColumnUtil.createColumn(type), Boolean.class)).isFalse();
    }

    private TypeInformation createType(int length, SqlServerType serverType) {
        return builder().withMaxLength(length).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withPrecision(length).withServerType(serverType).build();
    }
}
