/*
 * Copyright 2018-2019 the original author or authors.
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
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LocalTimeCodec}.
 *
 * @author Mark Paluch
 */
class LocalTimeCodecUnitTests {

    static final TypeInformation TIME = builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withScale(7).withServerType(SqlServerType.TIME).build();

    @Test
    void shouldEncodeTime() {

        LocalTime value = LocalTime.parse("18:13:14");

        Encoded encoded = LocalTimeCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), value);

        EncodedAssert.assertThat(encoded).isEqualToHex("07 05 00 19 12 B9 98");
        assertThat(encoded.getFormalType()).isEqualTo("time");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = LocalTimeCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("07 00");
        assertThat(encoded.getFormalType()).isEqualTo("time");
    }

    @Test
    void shouldDecodeTime() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("05 c0 c9 b1 61 5d");

        LocalTime decoded = LocalTimeCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(TIME), LocalTime.class);

        assertThat(decoded).isEqualTo("11:08:27.100");
    }

    @Test
    void shouldDecodeNull() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("00");

        LocalTime decoded = LocalTimeCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(TIME), LocalTime.class);

        assertThat(decoded).isNull();
    }
}
