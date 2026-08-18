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

import com.microsoft.sqlserver.jdbc.Geometry;
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
 * Unit tests for {@link GeometryCodec}.
 */
public class GeometryCodecUnitTests {

    static final TypeInformation GEOMETRY = builder().withLengthStrategy(LengthStrategy.PARTLENTYPE).withServerType(SqlServerType.GEOMETRY).build();

    @Test
    void shouldEncodeGeometry() throws SQLServerException {

        byte[] serialized = Geometry.STGeomFromText("POINT(30 10)", 0).serialize();
        Encoded encoded = GeometryCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), Geometry.STGeomFromText("POINT(30 10)", 0));

        EncodedAssert.assertThat(encoded).isEncodedAs(expected -> {
            encodeTypeInformation(expected);
            PlpLength.of(serialized.length).encode(expected);
            Length.of(serialized.length).encode(expected, LengthStrategy.PARTLENTYPE);
            expected.writeBytes(serialized);
            Length.of(0).encode(expected, LengthStrategy.PARTLENTYPE);
        });
        assertThat(encoded.getDataType()).isEqualTo(TdsDataType.UDT);
        assertThat(encoded.getFormalType()).isEqualTo("geometry");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = GeometryCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEncodedAs(expected -> {
            encodeTypeInformation(expected);
            PlpLength.nullLength().encode(expected);
        });
        assertThat(encoded.getDataType()).isEqualTo(TdsDataType.UDT);
        assertThat(encoded.getFormalType()).isEqualTo("geometry");
    }

    @Test
    void shouldBeAbleToEncodeNull() {

        assertThat(GeometryCodec.INSTANCE.canEncodeNull(Geometry.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodeGeometry() {

        assertThat(GeometryCodec.INSTANCE.canDecode(ColumnUtil.createColumn(GEOMETRY), Geometry.class)).isTrue();
    }

    @Test
    void shouldBeAbleToDecodePlpStream() throws SQLServerException {

        Geometry geometryVal = Geometry.STGeomFromText("POINT(30 10)", 0);
        ByteBuf buffer = TdsEncoded.plpStream(GEOMETRY, geometryVal.serialize(), 8, 7, 7);

        Geometry geometryData = GeometryCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(GEOMETRY), Geometry.class);

        assertThat(geometryData.STAsText()).isEqualTo(geometryVal.STAsText());
        assertThat(geometryData.getSrid()).isEqualTo(geometryVal.getSrid());
    }

    @Test
    void shouldRejectMalformedGeometry() {

        ByteBuf buffer = TdsEncoded.plpStream(GEOMETRY, new byte[]{0});

        assertThatThrownBy(() -> GeometryCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(GEOMETRY), Geometry.class))
                .isInstanceOf(SpatialDatatypeDecodeException.class)
                .hasCauseInstanceOf(SQLServerException.class);
    }

    private static void encodeTypeInformation(ByteBuf buffer) {
        TdsEncoded.encodeUnicodeBString(buffer, "master");
        TdsEncoded.encodeUnicodeBString(buffer, "sys");
        TdsEncoded.encodeUnicodeBString(buffer, "geometry");
    }

}
