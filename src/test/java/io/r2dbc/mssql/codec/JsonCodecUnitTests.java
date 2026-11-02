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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.codec.RpcParameterContext.ValueContext;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.message.type.*;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link JsonCodec}.
 */
class JsonCodecUnitTests {

    private static final String JSON = "{\"a\":1}";

    @Test
    void shouldEncodeNvarchar() {

        Collation collation = Collation.from(13632521, 52);

        Encoded encoded = JsonCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(new RpcParameterContext.CharacterValueContext(collation, true)), Json.of(JSON));

        EncodedAssert.assertThat(encoded).isEncodedAs(expected ->
        {
            expected.writeShortLE(8000); // max size

            // collation windows-1252
            expected.writeByte(0x09);
            expected.writeByte(0x04);
            expected.writeByte(0xD0);
            expected.writeByte(0x00);
            expected.writeByte(0x34);

            expected.writeShortLE(14); // actual size

            expected.writeCharSequence(JSON, ServerCharset.UNICODE.charset());
        });
        assertThat(encoded.getFormalType()).isEqualTo("nvarchar(4000)");
    }

    @Test
    void shouldEncodeVarchar() {

        Collation collation = Collation.from(13632521, 52);

        Encoded encoded = JsonCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(ValueContext.character(collation, false)), Json.of(JSON));

        EncodedAssert.assertThat(encoded).isEncodedAs(expected ->
        {
            expected.writeShortLE(8000); // max size

            // collation windows-1252
            expected.writeByte(0x09);
            expected.writeByte(0x04);
            expected.writeByte(0xD0);
            expected.writeByte(0x00);
            expected.writeByte(0x34);

            expected.writeShortLE(7); // actual size

            expected.writeCharSequence(JSON, ServerCharset.CP1252.charset());
        });
        assertThat(encoded.getFormalType()).isEqualTo("varchar(8000)");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = JsonCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("40 1f 00 00 00 00 00 ff ff");
        assertThat(encoded.getFormalType()).isEqualTo("varchar(8000)");
    }

    @Test
    void shouldEncodeOnlyJson() {

        assertThat(JsonCodec.INSTANCE.canEncode(Json.of(JSON))).isTrue();
        assertThat(JsonCodec.INSTANCE.canEncode(JSON)).isFalse();
        assertThat(JsonCodec.INSTANCE.canEncodeNull(Json.class)).isTrue();
        assertThat(JsonCodec.INSTANCE.canEncodeNull(String.class)).isFalse();
    }

    @Test
    void shouldDecodeOnlyWhenJsonIsRequested() {

        TypeInformation type =
                builder().withMaxLength(50).withLengthStrategy(LengthStrategy.PARTLENTYPE).withPrecision(50).withServerType(SqlServerType.NVARCHARMAX).withCharset(ServerCharset.UNICODE.charset()).build();

        assertThat(JsonCodec.INSTANCE.canDecode(ColumnUtil.createColumn(type), Json.class)).isTrue();
        assertThat(JsonCodec.INSTANCE.canDecode(ColumnUtil.createColumn(type), String.class)).isFalse();
    }

    @Test
    void shouldNotDecodeNonCharacterTypes() {

        TypeInformation type = builder().withMaxLength(4).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withPrecision(4).withServerType(SqlServerType.INTEGER).build();

        assertThat(JsonCodec.INSTANCE.canDecode(ColumnUtil.createColumn(type), Json.class)).isFalse();
    }

    @Test
    void shouldDecodeVarcharMax() {

        TypeInformation type =
                builder().withMaxLength(50).withLengthStrategy(LengthStrategy.PARTLENTYPE).withPrecision(50).withServerType(SqlServerType.VARCHAR).withCharset(ServerCharset.CP1252.charset()).build();

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        Encode.uLongLong(data, 7);
        Encode.asInt(data, 7);
        data.writeCharSequence(JSON, ServerCharset.CP1252.charset());

        Json value = JsonCodec.INSTANCE.decode(data, ColumnUtil.createColumn(type), Json.class);

        assertThat(value).isNotNull();
        assertThat(value.asString()).isEqualTo(JSON);
    }

    @Test
    void shouldDecodeNvarchar() {

        TypeInformation type =
                builder().withMaxLength(100).withLengthStrategy(LengthStrategy.USHORTLENTYPE).withPrecision(50).withServerType(SqlServerType.VARCHAR).withCharset(ServerCharset.UNICODE.charset()).build();

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        Encode.uShort(data, 14);
        data.writeCharSequence(JSON, ServerCharset.UNICODE.charset());

        Json value = JsonCodec.INSTANCE.decode(data, ColumnUtil.createColumn(type), Json.class);

        assertThat(value).isNotNull();
        assertThat(value.asString()).isEqualTo(JSON);
    }

    @Test
    void shouldDecodeVarcharMaxWithUnknownLength() {

        TypeInformation type =
                builder().withMaxLength(50).withLengthStrategy(LengthStrategy.PARTLENTYPE).withPrecision(50).withServerType(SqlServerType.VARCHAR).withCharset(ServerCharset.CP1252.charset()).build();

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        PlpLength.unknown().encode(data);
        Encode.asInt(data, 7);
        data.writeCharSequence(JSON, ServerCharset.CP1252.charset());
        Encode.asInt(data, 0);

        Json value = JsonCodec.INSTANCE.decode(data, ColumnUtil.createColumn(type), Json.class);

        assertThat(value).isNotNull();
        assertThat(value.asString()).isEqualTo(JSON);
    }

    @Test
    void shouldDecodeNull() {

        TypeInformation type =
                builder().withMaxLength(50).withLengthStrategy(LengthStrategy.PARTLENTYPE).withPrecision(50).withServerType(SqlServerType.NVARCHARMAX).withCharset(ServerCharset.UNICODE.charset()).build();

        assertThat(JsonCodec.INSTANCE.decode(null, ColumnUtil.createColumn(type), Json.class)).isNull();
    }

}