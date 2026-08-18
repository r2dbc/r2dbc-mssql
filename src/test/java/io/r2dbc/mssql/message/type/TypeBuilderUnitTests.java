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

package io.r2dbc.mssql.message.type;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TdsEncoded;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Paluch
 */
class TypeBuilderUnitTests {

    @Test
    void shouldDecodeInt() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("000000000800380B");
        TypeInformation typeInformation = TypeBuilder.decode(buffer, true);

        assertThat(typeInformation.getMaxLength()).isEqualTo(4);
        assertThat(typeInformation.getServerType()).isEqualTo(SqlServerType.INTEGER);
        assertThat(typeInformation.getLengthStrategy()).isEqualTo(LengthStrategy.FIXEDLENTYPE);
        assertThat(typeInformation.getDisplaySize()).isEqualTo(11);
    }

    @Test
    void canDecodeShouldCheckDecodingAbility() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("000000000800380B");

        assertThat(TypeBuilder.canDecode(buffer, true)).isTrue();
        assertThat(buffer.readerIndex()).isEqualTo(0);

        assertThat(TypeBuilder.canDecode(HexUtils.decodeToByteBuf("000000000800"), true)).isFalse();
    }

    @Test
    void shouldDecodeSpatialTypeInfo() {

        ByteBuf buffer = spatialTypeInfo("geometry");

        TypeInformation typeInformation = TypeBuilder.decode(buffer, true);

        assertThat(typeInformation.getServerType()).isEqualTo(SqlServerType.GEOMETRY);
        assertThat(typeInformation.getLengthStrategy()).isEqualTo(LengthStrategy.PARTLENTYPE);
        assertThat(buffer.readableBytes()).isEqualTo(0);
    }

    @Test
    void canDecodeShouldCheckSpatialTypeInfoDecodingAbility() {

        ByteBuf buffer = spatialTypeInfo("geography");
        int writerIndex = buffer.writerIndex();

        assertThat(TypeBuilder.canDecode(buffer, true)).isTrue();
        assertThat(buffer.readerIndex()).isEqualTo(0);

        for (int i = 1; i < writerIndex; i++) {

            buffer.writerIndex(writerIndex - i);
            assertThat(TypeBuilder.canDecode(buffer, true)).describedAs("canDecode() with missing " + i + " bytes").isFalse();
            assertThat(buffer.readerIndex()).isEqualTo(0);
        }
    }

    private static ByteBuf spatialTypeInfo(String udtTypeName) {

        ByteBuf buffer = Unpooled.buffer();
        buffer.writeInt(0); // user type
        buffer.writeShortLE(0); // flags
        buffer.writeByte(TdsDataType.UDT.getValue());
        buffer.writeShortLE(0); // max byte size
        TdsEncoded.encodeUnicodeBString(buffer, ""); // database name
        TdsEncoded.encodeUnicodeBString(buffer, ""); // schema name
        TdsEncoded.encodeUnicodeBString(buffer, udtTypeName);
        buffer.writeShortLE(0); // assembly qualified name (empty)
        return buffer;
    }
}
