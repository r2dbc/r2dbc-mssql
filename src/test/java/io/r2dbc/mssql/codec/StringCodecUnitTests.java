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
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link StringCodec}.
 *
 * @author Mark Paluch
 */
class StringCodecUnitTests {

    @Test
    void shouldEncodeNvarchar() {

        Collation collation = Collation.from(13632521, 52);

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        Encode.uShort(data, 12);
        data.writeCharSequence("foobar", ServerCharset.UNICODE.charset());

        Encoded encoded = StringCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(collation), "foobar");

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

            expected.writeCharSequence("foobar", ServerCharset.UNICODE.charset());
        });
        assertThat(encoded.getFormalType()).isEqualTo("nvarchar(4000)");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = StringCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("40 1f 00 00 00 00 00 ff ff");
        assertThat(encoded.getFormalType()).isEqualTo("nvarchar(4000)");
    }

    @Test
    void shouldBeAbleToDecodeUuid() {

        TypeInformation type = builder().withMaxLength(16).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withPrecision(16).withServerType(SqlServerType.GUID).build();

        assertThat(StringCodec.INSTANCE.canDecode(ColumnUtil.createColumn(type), String.class)).isTrue();
    }

    @Test
    void shouldDecodeUuid() {

        TypeInformation type = builder().withMaxLength(16).withLengthStrategy(LengthStrategy.FIXEDLENTYPE).withPrecision(16).withServerType(SqlServerType.GUID).build();
        ByteBuf buffer = HexUtils.decodeToByteBuf("F17B0DC7C7E5C54098C7A12F7E686724FD");

        String value = StringCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(type), String.class);

        assertThat(value).isEqualTo("C70D7BF1-E5C7-40C5-98C7-A12F7E686724");
    }

    @Test
    void shouldDecodeVarchar() {

        TypeInformation type =
            builder().withMaxLength(50).withLengthStrategy(LengthStrategy.USHORTLENTYPE).withPrecision(50).withServerType(SqlServerType.VARCHAR).withCharset(ServerCharset.CP1252.charset()).build();

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        Encode.uShort(data, 6);
        data.writeCharSequence("foobar", ServerCharset.CP1252.charset());


        String value = StringCodec.INSTANCE.decode(data, ColumnUtil.createColumn(type), String.class);

        assertThat(value).isEqualTo("foobar");
    }

    @Test
    void shouldDecodeNvarchar() {

        TypeInformation type =
            builder().withMaxLength(100).withLengthStrategy(LengthStrategy.USHORTLENTYPE).withPrecision(50).withServerType(SqlServerType.VARCHAR).withCharset(ServerCharset.UNICODE.charset()).build();

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        Encode.uShort(data, 12);
        data.writeCharSequence("foobar", ServerCharset.UNICODE.charset());

        String value = StringCodec.INSTANCE.decode(data, ColumnUtil.createColumn(type), String.class);

        assertThat(value).isEqualTo("foobar");
    }

    @Test
    void shouldDecodeChar() {

        TypeInformation type =
            builder().withMaxLength(20).withLengthStrategy(LengthStrategy.USHORTLENTYPE).withServerType(SqlServerType.CHAR).withCharset(ServerCharset.CP1252.charset()).build();

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        Encode.uShort(data, 20);
        data.writeCharSequence("foobar              ", ServerCharset.CP1252.charset());

        String value = StringCodec.INSTANCE.decode(data, ColumnUtil.createColumn(type), String.class);

        assertThat(value).isEqualTo("foobar              ");
    }

    @Test
    void shouldDecodeText() {

        TypeInformation type =
            builder().withMaxLength(2147483647).withLengthStrategy(LengthStrategy.LONGLENTYPE).withServerType(SqlServerType.TEXT).withCharset(ServerCharset.CP1252.charset()).build();

        // Text value
        ByteBuf data = HexUtils.decodeToByteBuf("10 64" +
            "75 6D 6D 79 20 74 65 78 74 70 74 72 00 00 00 64" +
            "75 6D 6D 79 54 53 00 0B 00 00 00 6D 79 74 65 78" +
            "74 76 61 6C 75 65");

        String value = StringCodec.INSTANCE.decode(data, ColumnUtil.createColumn(type), String.class);

        assertThat(value).isEqualTo("mytextvalue");
    }
}
