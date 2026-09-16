/*
 * Copyright 2026 the original author or authors.
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

package io.r2dbc.mssql.message.tds;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.HexUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PlpBuffer}.
 *
 * @author Mark Paluch
 */
class PlpBufferUnitTests {

    TypeInformation VARCHARMAX = TypeInformation.builder().withServerType(SqlServerType.VARCHARMAX).withLengthStrategy(LengthStrategy.PARTLENTYPE).withCharset(StandardCharsets.US_ASCII).build();

    @Test
    void decodeMapShouldReleaseChunksOnMalformedStream() {

        // second chunk announces more bytes than available
        ByteBuf buffer = HexUtils.decodeToByteBuf("0400000000000000 02000000 6162 10000000 6364");
        PlpBuffer plpBuffer = PlpBuffer.of(buffer, VARCHARMAX);
        plpBuffer.decodeLength();

        assertThatThrownBy(plpBuffer::decodeByteArray).isInstanceOf(IndexOutOfBoundsException.class);
        assertThat(buffer.refCnt()).isOne();
    }

}
