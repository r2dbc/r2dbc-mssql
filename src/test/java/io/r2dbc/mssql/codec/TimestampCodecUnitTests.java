/*
 * Copyright 2018-2020 the original author or authors.
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
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link TimestampCodec}.
 *
 * @author Mark Paluch
 */
class TimestampCodecUnitTests {

    @Test
    void shouldEncodeTimestamp() {

        byte[] value = new byte[]{0, 0, 0, 0, 0, 0, 7, -47};

        assertThatThrownBy(() -> TimestampCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), value)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldDecodeTimestamp() {

        TypeInformation type = TypeInformation.builder().withLengthStrategy(LengthStrategy.USHORTLENTYPE).withServerType(SqlServerType.TIMESTAMP).build();

        ByteBuf buffer = HexUtils.decodeToByteBuf("080000000000000007D1");

        byte[] decoded = TimestampCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), byte[].class);

        assertThat(decoded).containsSequence(0, 0, 0, 0, 0, 0, 7, -47);
    }
}
