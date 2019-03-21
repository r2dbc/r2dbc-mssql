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
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static io.r2dbc.mssql.codec.ColumnUtil.createColumn;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MoneyCodec}.
 *
 * @author Mark Paluch
 */
class MoneyCodecUnitTests {

    @Test
    void shouldEncodeMoney() {

        Encoded encoded = MoneyCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), new BigDecimal("7301494.4032"));

        EncodedAssert.assertThat(encoded).isEqualToHex("08 08 11 00 00 00 20 a1 07 00");
        assertThat(encoded.getFormalType()).isEqualTo("money");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = MoneyCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("08 00");
        assertThat(encoded.getFormalType()).isEqualTo("money");
    }

    @Test
    void shouldDecodeBigMoney() {

        Column column =
            createColumn(TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.MONEY).withMaxLength(8).build());

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer(9);
        buffer.writeByte(8);
        Encode.money(buffer, new BigDecimal("7301494.4032").unscaledValue());

        BigDecimal decoded = MoneyCodec.INSTANCE.decode(buffer, column, BigDecimal.class);

        assertThat(decoded).isEqualTo("7301494.4032");
    }

    @Test
    void shouldDecodeSmallMoney() {

        Column column =
            createColumn(TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.SMALLMONEY).withMaxLength(4).build());

        ByteBuf buffer = HexUtils.decodeToByteBuf("0420A10500");

        BigDecimal decoded = MoneyCodec.INSTANCE.decode(buffer, column, BigDecimal.class);

        assertThat(decoded).isEqualTo("36.8928");
    }
}
