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
import io.r2dbc.mssql.message.type.TypeInformation.LengthStrategy;
import io.r2dbc.mssql.message.type.TypeInformation.SqlServerType;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FloatCodec}.
 *
 * @author Mark Paluch
 */
class FloatCodecUnitTests {

    @Test
    void shouldBeAbleToDecode() {

        TypeInformation floatType =
            builder().withServerType(SqlServerType.FLOAT).build();

        TypeInformation intType =
            builder().withServerType(SqlServerType.INTEGER).build();

        assertThat(FloatCodec.INSTANCE.canDecode(ColumnUtil.createColumn(floatType), Float.class)).isTrue();
        assertThat(FloatCodec.INSTANCE.canDecode(ColumnUtil.createColumn(intType), Float.class)).isFalse();
    }

    @Test
    void shouldDecodeFloat() {

        TypeInformation type = builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.FLOAT).withMaxLength(8).build();

        ByteBuf buffer = HexUtils.decodeToByteBuf("08FED478E94628C640");

        assertThat(FloatCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Float.class)).isEqualTo((float) 11344.554);
    }

    @Test
    void shouldDecodeReal() {

        TypeInformation type = builder().withLengthStrategy(LengthStrategy.BYTELENTYPE).withServerType(SqlServerType.REAL).withMaxLength(4).build();

        ByteBuf buffer = HexUtils.decodeToByteBuf("0437423146");

        assertThat(FloatCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), Float.class)).isEqualTo((float) 11344.554);
    }

    @Test
    void shouldEncodeFloat() {

        Encoded encoded = FloatCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), 11344.554f);

        EncodedAssert.assertThat(encoded).isEqualToHex("37 42 31 46");
    }
}
