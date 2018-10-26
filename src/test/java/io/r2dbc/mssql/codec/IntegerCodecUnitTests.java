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
 * Unit tests for {@link IntegerCodec}.
 *
 * @author Mark Paluch
 */
class IntegerCodecUnitTests {

    @Test
    void shouldBeAbleToDecode() {

        TypeInformation tinyint =
            builder().withServerType(SqlServerType.TINYINT).build();

        TypeInformation varchar =
            builder().withServerType(SqlServerType.VARCHAR).build();

        assertThat(IntegerCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), Integer.class)).isTrue();
        assertThat(IntegerCodec.INSTANCE.canDecode(ColumnUtil.createColumn(varchar), Integer.class)).isFalse();
        assertThat(IntegerCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), String.class)).isFalse();
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

    @Test
    void shouldEncodeInteger() {

        TypeInformation type = createType(4, SqlServerType.INTEGER);

        EncodedAssert.assertThat(IntegerCodec.INSTANCE.encode(TestByteBufAllocator.TEST, type, 16777217)).isEqualToHex("0100000100000000");
    }
}
