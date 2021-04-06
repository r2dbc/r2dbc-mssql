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

import java.math.BigInteger;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BigIntegerCodec}.
 *
 * @author Mark Paluch
 */
class BigIntegerCodecUnitTests {

    @Test
    void shouldEncodeInteger() {

        Encoded encoded = BigIntegerCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), new BigInteger("12345"));

        EncodedAssert.assertThat(encoded).isEqualToHex("11 26 00 03 01 39 30");
        assertThat(encoded.getFormalType()).isEqualTo("decimal(38,0)");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = BigIntegerCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("08 00");
        assertThat(encoded.getFormalType()).isEqualTo("bigint");
    }

    @Test
    void shouldBeAbleToDecode() {

        TypeInformation tinyint =
            builder().withServerType(SqlServerType.TINYINT).build();

        TypeInformation varchar =
            builder().withServerType(SqlServerType.VARCHAR).build();

        TypeInformation numeric =
            builder().withServerType(SqlServerType.NUMERIC).build();

        assertThat(BigIntegerCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), BigInteger.class)).isTrue();
        assertThat(BigIntegerCodec.INSTANCE.canDecode(ColumnUtil.createColumn(varchar), BigInteger.class)).isFalse();
        assertThat(BigIntegerCodec.INSTANCE.canDecode(ColumnUtil.createColumn(tinyint), BigInteger.class)).isTrue();
        assertThat(BigIntegerCodec.INSTANCE.canDecode(ColumnUtil.createColumn(numeric), BigInteger.class)).isTrue();
    }

    @Test
    void shouldDecodeFromLong() {

        TypeInformation type = createType(8, SqlServerType.BIGINT);

        ByteBuf buffer = HexUtils.decodeToByteBuf("0100000100000000");

        assertThat(BigIntegerCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), BigInteger.class)).isEqualTo(BigInteger.valueOf(16777217));
    }

    @Test
    void shouldDecodeFromNumeric() {

        TypeInformation type = TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.NUMERIC).withScale(0).withPrecision(5).build();

        ByteBuf buffer = HexUtils.decodeToByteBuf("05 01 39 30 00 00");

        assertThat(BigIntegerCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), BigInteger.class)).isEqualTo(BigInteger.valueOf(12345));
    }

    private TypeInformation createType(int length, SqlServerType serverType) {
        return builder().withMaxLength(length).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withPrecision(length).withServerType(serverType).build();
    }
}
