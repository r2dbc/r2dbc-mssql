/*
 * Copyright 2018-2022 the original author or authors.
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

import com.microsoft.sqlserver.jdbc.Geography;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.type.*;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.TdsEncoded;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link GeographyCodec}.
 */
public class GeographyCodecUnitTests {

    static final TypeInformation GEOGRAPHY = builder().withLengthStrategy(LengthStrategy.PARTLENTYPE).withServerType(SqlServerType.GEOGRAPHY).build();

    @Test
    void shouldEncodeGeography() throws SQLServerException {

        byte[] serialized = Geography.STGeomFromText("POINT(144.9631 -37.8136)", 4326).serialize();
        Encoded encoded = GeographyCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), Geography.STGeomFromText("POINT(144.9631 -37.8136)", 4326));

        EncodedAssert.assertThat(encoded).isEncodedAs(expected -> {
            encodeTypeInformation(expected);
            PlpLength.of(serialized.length).encode(expected);
            Length.of(serialized.length).encode(expected, LengthStrategy.PARTLENTYPE);
            expected.writeBytes(serialized);
            Length.of(0).encode(expected, LengthStrategy.PARTLENTYPE);
        });
        assertThat(encoded.getDataType()).isEqualTo(TdsDataType.UDT);
        assertThat(encoded.getFormalType()).isEqualTo("geography");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = GeographyCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEncodedAs(expected -> {
            encodeTypeInformation(expected);
            PlpLength.nullLength().encode(expected);
        });
        assertThat(encoded.getDataType()).isEqualTo(TdsDataType.UDT);
        assertThat(encoded.getFormalType()).isEqualTo("geography");
    }

    @Test
    void shouldBeAbleToEncodeNull() {

        assertThat(GeographyCodec.INSTANCE.canEncodeNull(Geography.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeGeography() {

        assertThat(GeographyCodec.INSTANCE.canDecode(ColumnUtil.createColumn(GEOGRAPHY), Geography.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodePlpStream() throws SQLServerException {

        Geography geographyVal = Geography.STGeomFromText("POINT(144.9631 -37.8136)", 4326);
        ByteBuf buffer = TdsEncoded.plpStream(GEOGRAPHY, geographyVal.serialize(), 8, 7, 7);

        Geography geographyData = GeographyCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(GEOGRAPHY), Geography.class);

        assertThat(geographyData.STAsText()).isEqualTo(geographyVal.STAsText());
        assertThat(geographyData.getSrid()).isEqualTo(geographyVal.getSrid());
    }

    @Test
    void shouldRejectMalformedGeography() {

        ByteBuf buffer = TdsEncoded.plpStream(GEOGRAPHY, new byte[]{0});

        assertThatThrownBy(() -> GeographyCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(GEOGRAPHY), Geography.class))
                .isInstanceOf(SpatialDatatypeDecodeException.class)
                .hasCauseInstanceOf(SQLServerException.class);
    }

    private static void encodeTypeInformation(ByteBuf buffer) {
        TdsEncoded.encodeUnicodeBString(buffer, "master");
        TdsEncoded.encodeUnicodeBString(buffer, "sys");
        TdsEncoded.encodeUnicodeBString(buffer, "geography");
    }

}
