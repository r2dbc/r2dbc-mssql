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

import java.util.UUID;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link UuidCodec}.
 *
 * @author Mark Paluch
 */
class UuidCodecUnitTests {

    @Test
    void shouldEncodeUuid() {

        UUID uuid = UUID.fromString("C70D7BF1-E5C7-40C5-98C7-A12F7E686724");
        Encoded encoded = UuidCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), uuid);

        EncodedAssert.assertThat(encoded).isEqualToHex("10 10 F17B0DC7C7E5C54098C7A12F7E686724");
        assertThat(encoded.getFormalType()).isEqualTo("uniqueidentifier");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = UuidCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("10 00");
        assertThat(encoded.getFormalType()).isEqualTo("uniqueidentifier");
    }

    @Test
    void shouldDecodeUuid() {

        TypeInformation type = builder().withMaxLength(16).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withPrecision(16).withServerType(SqlServerType.GUID).build();
        ByteBuf buffer = HexUtils.decodeToByteBuf("F17B0DC7C7E5C54098C7A12F7E686724");

        UUID decoded = UuidCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), UUID.class);
        assertThat(decoded).isEqualTo(UUID.fromString("C70D7BF1-E5C7-40C5-98C7-A12F7E686724"));
    }
}
