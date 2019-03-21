/*
 * Copyright 2019-2019 the original author or authors.
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

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link OffsetDateTimeCodec}.
 *
 * @author Mark Paluch
 */
class OffsetDateTimeCodecUnitTests {

    static final TypeInformation DATETIMEOFFSET = TypeInformation.builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withScale(7).withServerType(SqlServerType.DATETIMEOFFSET).build();

    @Test
    void shouldEncodeDatetimeoffset() {

        OffsetDateTime value = OffsetDateTime.parse("2018-08-27T17:41:14.890+00:45");

        Encoded encoded = OffsetDateTimeCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), value);

        EncodedAssert.assertThat(encoded).isEqualToHex("07 0a a0 d8 dd f7 8d a4 3e 0b 2d 00");
        assertThat(encoded.getFormalType()).isEqualTo("datetimeoffset");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = OffsetDateTimeCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("07 00");
        assertThat(encoded.getFormalType()).isEqualTo("datetimeoffset");
    }

    @Test
    void shouldDecodeDateTimeOffset() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("0a a0 d8 dd f7 8d a4 3e 0b 2d 00");

        OffsetDateTime decoded = OffsetDateTimeCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(DATETIMEOFFSET), OffsetDateTime.class);

        assertThat(decoded).isEqualTo("2018-08-27T17:41:14.890+00:45");
    }
}
