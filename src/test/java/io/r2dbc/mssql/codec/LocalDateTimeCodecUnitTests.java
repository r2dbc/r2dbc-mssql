/*
 * Copyright 2018 the original author or authors.
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

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LocalDateTimeCodec}.
 *
 * @author Mark Paluch
 */
class LocalDateTimeCodecUnitTests {

    static final TypeInformation SMALLDATETIME = TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.SMALLDATETIME).build();

    static final TypeInformation DATETIME = TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.DATETIME).build();

    static final TypeInformation DATETIME2 = TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withScale(7).withServerType(SqlServerType.DATETIME2).build();

    @Test
    void shouldDecodeSmallDateTime() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("04F5A83E00");

        LocalDateTime decoded = LocalDateTimeCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(SMALLDATETIME), LocalDateTime.class);

        assertThat(decoded).isEqualTo("2018-06-04T01:02");
    }

    @Test
    void shouldEncodeSmallDateTime() {

        LocalDateTime value = LocalDateTime.parse("2018-06-04T01:02");

        ByteBuf encoded = TestByteBufAllocator.TEST.buffer();

        LocalDateTimeCodec.encode(encoded, SqlServerType.SMALLDATETIME, 0, value);

        EncodedAssert.assertThat(encoded).isEqualToHex("F5A83E00");
    }

    @Test
    void shouldDecodeDateTime() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("0886A90000AA700201");

        LocalDateTime decoded = LocalDateTimeCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(DATETIME), LocalDateTime.class);

        assertThat(decoded).isEqualTo("2018-10-27T15:40:57.1");
    }

    @Test
    void shouldEncodeDateTime() {

        LocalDateTime value = LocalDateTime.parse("2018-10-27T15:40:57.1");

        ByteBuf encoded = TestByteBufAllocator.TEST.buffer();
        LocalDateTimeCodec.encode(encoded, SqlServerType.DATETIME, 0, value);
        EncodedAssert.assertThat(encoded).isEqualToHex("86A90000AA700201");
    }

    @Test
    void shouldDecodeDateTime2() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("082006E17483E13E0B");

        LocalDateTime decoded = LocalDateTimeCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(DATETIME2), LocalDateTime.class);

        assertThat(decoded).isEqualTo("2018-10-27T15:41:00.162");
    }

    @Test
    void shouldEncodeDateTime2() {

        LocalDateTime value = LocalDateTime.parse("2018-10-27T15:41:00.162");

        Encoded encoded = LocalDateTimeCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), value);

        EncodedAssert.assertThat(encoded).isEqualToHex("07 08 20 06 E1 74 83 E1 3E 0B");
        assertThat(encoded.getFormalType()).isEqualTo("datetime2");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = LocalDateTimeCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("07 00");
        assertThat(encoded.getFormalType()).isEqualTo("datetime2");
    }
}
