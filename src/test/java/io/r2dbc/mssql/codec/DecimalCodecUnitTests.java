/*
 * Copyright 2018-2019 the original author or authors.
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

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DecimalCodec}.
 *
 * @author Mark Paluch
 */
class DecimalCodecUnitTests {

    @Test
    void shouldEncodeNull() {

        Encoded encoded = DecimalCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("11 26 00 00");
        assertThat(encoded.getFormalType()).isEqualTo("decimal(38,0)");
    }

    @Test
    void shouldEncodeNumeric5x2() {

        Encoded encoded = DecimalCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), new BigDecimal("36.89"));

        EncodedAssert.assertThat(encoded).isEqualToHex("11 26 02 03 01 69 0e");
        assertThat(encoded.getFormalType()).isEqualTo("decimal(38,2)");
    }

    @Test
    void shouldDecodeNumeric5x2() {

        TypeInformation type = TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.NUMERIC).withScale(2).withPrecision(5).build();

        ByteBuf buffer = HexUtils.decodeToByteBuf("0501690E0000");

        BigDecimal decoded = DecimalCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), BigDecimal.class);

        assertThat(decoded).isEqualTo("36.89");
    }
}
