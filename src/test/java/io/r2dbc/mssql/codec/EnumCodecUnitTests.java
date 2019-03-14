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
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link EnumCodec}.
 *
 * @author Stephan Dreyer
 */
class EnumCodecUnitTests {

    @Test
    void shouldEncodeNvarchar() {

        Collation collation = Collation.from(13632521, 52);

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        Encode.uShort(data, 12);
        data.writeCharSequence("FOOBAR", ServerCharset.UNICODE.charset());

        Encoded encoded = EnumCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(collation), TestEnum.FOOBAR);

        EncodedAssert.assertThat(encoded).isEncodedAs(expected ->
        {
            expected.writeShortLE(8000); // max size

            // collation windows-1252
            expected.writeByte(0x00);
            expected.writeByte(0xD0);
            expected.writeByte(0x04);
            expected.writeByte(0x09);
            expected.writeByte(0x34);

            expected.writeShortLE(12); // actual size

            expected.writeCharSequence("FOOBAR", ServerCharset.UNICODE.charset());
        });
        assertThat(encoded.getFormalType()).isEqualTo("nvarchar(4000)");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = EnumCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("40 1f 00 00 00 00 00 ff ff");
        assertThat(encoded.getFormalType()).isEqualTo("nvarchar(4000)");
    }

    @Test
    void shouldDecodeVarchar() {

        TypeInformation type =
            builder().withMaxLength(50).withLengthStrategy(LengthStrategy.USHORTLENTYPE).withPrecision(50).withServerType(SqlServerType.VARCHAR).withCharset(ServerCharset.CP1252.charset()).build();

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        Encode.uShort(data, 6);
        data.writeCharSequence("FOOBAR", ServerCharset.CP1252.charset());


        Enum<?> value = EnumCodec.INSTANCE.decode(data, ColumnUtil.createColumn(type), TestEnum.class);

        assertThat(value).isEqualTo(TestEnum.FOOBAR);
    }

    @Test
    void shouldDecodeNvarchar() {

        TypeInformation type =
            builder().withMaxLength(100).withLengthStrategy(LengthStrategy.USHORTLENTYPE).withPrecision(50).withServerType(SqlServerType.VARCHAR).withCharset(ServerCharset.UNICODE.charset()).build();

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        Encode.uShort(data, 12);
        data.writeCharSequence("FOOBAR", ServerCharset.UNICODE.charset());

        Enum<?> value = EnumCodec.INSTANCE.decode(data, ColumnUtil.createColumn(type), TestEnum.class);

        assertThat(value).isEqualTo(TestEnum.FOOBAR);
    }

    @Test
    void shouldDecodeChar() {

        TypeInformation type =
            builder().withMaxLength(20).withLengthStrategy(LengthStrategy.USHORTLENTYPE).withServerType(SqlServerType.CHAR).withCharset(ServerCharset.CP1252.charset()).build();

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        Encode.uShort(data, 20);
        data.writeCharSequence("FOOBAR              ", ServerCharset.CP1252.charset());

        Enum<?> value = EnumCodec.INSTANCE.decode(data, ColumnUtil.createColumn(type), TestEnum.class);

        assertThat(value).isEqualTo(TestEnum.FOOBAR);
    }

    enum TestEnum {
        FOOBAR
    }
}
