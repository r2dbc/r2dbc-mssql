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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TdsEncoded;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ReturnValue}.
 *
 * @author Mark Paluch
 */
class ReturnValueUnitTests {

    @Test
    void shouldDecode() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("AC0000000100000000000026" +
            "0404F3DEBC0A");

        assertThat(buffer.readByte()).isEqualTo(ReturnValue.TYPE);

        ReturnValue returnValue = ReturnValue.decode(buffer, false);

        assertThat(returnValue.getOrdinal()).isEqualTo(0);
        assertThat(returnValue.getParameterName()).isEqualTo("");
        assertThat(returnValue.getStatus()).isEqualTo((byte) 1);
        assertThat(returnValue.getValueType().getServerType()).isEqualTo(SqlServerType.INTEGER);
        assertThat(returnValue.getValueType().getLengthStrategy()).isEqualTo(LengthStrategy.BYTELENTYPE);
        assertThat(returnValue.getValueType().getPrecision()).isEqualTo(10);
        assertThat(buffer.readerIndex()).isEqualTo(buffer.writerIndex());

        EncodedAssert.assertThat(returnValue.getValue()).isEncodedAs(expected -> {
            expected.writeByte(4); // length
            expected.writeIntLE(180150003);
        });
    }

    @Test
    void canDecodeShouldReportDecodability() {

        String data = "00000001000000000000260404F3DEBC0A";

        CanDecodeTestSupport.testCanDecode(HexUtils.decodeToByteBuf(data), buffer -> ReturnValue.canDecode(buffer, true));
    }

    @Test
    void shouldDecodeSpatialPlpValue() {

        ByteBuf buffer = spatialReturnValue("geometry");

        ReturnValue returnValue = ReturnValue.decode(buffer, false);

        assertThat(returnValue.getValueType().getServerType()).isEqualTo(SqlServerType.GEOMETRY);
        assertThat(returnValue.getValue().readableBytes()).isEqualTo(22);
        assertThat(buffer.readerIndex()).isEqualTo(buffer.writerIndex());

        EncodedAssert.assertThat(returnValue.getValue()).isEncodedAs(expected -> {
            expected.writeLongLE(2);
            expected.writeIntLE(1);
            expected.writeByte(0x11);
            expected.writeIntLE(1);
            expected.writeByte(0x22);
            expected.writeIntLE(0);
        });

        returnValue.release();
    }

    @Test
    void canDecodeShouldReportSpatialPlpDecodability() {
        CanDecodeTestSupport.testCanDecode(spatialReturnValue("geography"), buffer -> ReturnValue.canDecode(buffer, true));
    }

    private static ByteBuf spatialReturnValue(String udtTypeName) {

        ByteBuf buffer = Unpooled.buffer();
        buffer.writeShortLE(0); // ordinal
        TdsEncoded.encodeUnicodeBString(buffer, ""); // parameter name (empty)
        buffer.writeByte(1); // status
        buffer.writeInt(0); // user type
        buffer.writeShortLE(0); // flags
        buffer.writeByte(TdsDataType.UDT.getValue());
        buffer.writeShortLE(0); // max byte size
        TdsEncoded.encodeUnicodeBString(buffer, ""); // database name
        TdsEncoded.encodeUnicodeBString(buffer, ""); // schema name
        TdsEncoded.encodeUnicodeBString(buffer, udtTypeName);
        buffer.writeShortLE(0); // assembly qualified name (empty)
        buffer.writeLongLE(2); // PLP length
        buffer.writeIntLE(1);
        buffer.writeByte(0x11);
        buffer.writeIntLE(1);
        buffer.writeByte(0x22);
        buffer.writeIntLE(0); // PLP terminator
        return buffer;
    }
}
