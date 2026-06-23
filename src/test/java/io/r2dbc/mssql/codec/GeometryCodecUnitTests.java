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

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.Geometry;
import com.microsoft.sqlserver.jdbc.SQLServerException;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Unit tests for {@link GeometryCodec}.
 *
 * @author svats0001
 */
public class GeometryCodecUnitTests {

    static final TypeInformation GEOMETRY = builder().withLengthStrategy(LengthStrategy.PARTLENTYPE).withServerType(SqlServerType.GEOMETRY).build();
    
    @Test
    void shouldEncodeGeometry() throws SQLServerException {
        
        Encoded encoded = GeometryCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.out(), Geometry.STGeomFromText("POINT(30 10)", 0));
        
        EncodedAssert.assertThat(encoded).isEqualToHex("40 1F 16 00 00 00 00 00 01 0C 00 00 00 00 00 00 3E 40 00 00 00 00 00 00 24 40");
        assertThat(encoded.getFormalType()).isEqualTo("varbinary(8000)");
    }

    @Test
    void shouldEncodeNull() {

        Encoded encoded = GeometryCodec.INSTANCE.encodeNull(TestByteBufAllocator.TEST);

        EncodedAssert.assertThat(encoded).isEqualToHex("40 1F FF FF");
        assertThat(encoded.getFormalType()).isEqualTo("varbinary(8000)");
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
        byte[] geometryBytes = geometryVal.serialize();
        byte[] first = Arrays.copyOfRange(geometryBytes, 0, 8);
        byte[] second = Arrays.copyOfRange(geometryBytes, 8, 15);
        byte[] third = Arrays.copyOfRange(geometryBytes, 15, 22);

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer(20 + 22);
        PlpLength.of(22).encode(buffer);

        Length.of(8).encode(buffer, GEOMETRY);
        buffer.writeBytes(first);

        Length.of(7).encode(buffer, GEOMETRY);
        buffer.writeBytes(second);

        Length.of(7).encode(buffer, GEOMETRY);
        buffer.writeBytes(third);

        Geometry geometryData = GeometryCodec.INSTANCE.decode(buffer, ColumnUtil.createColumn(GEOMETRY), Geometry.class);

        StepVerifier.create(Mono.fromSupplier(() -> {
            try {
                return geometryData.STAsText() + geometryData.getSrid();
            } catch (SQLServerException e) {
                return null;
            }
        }))
            .expectNext(geometryVal.STAsText() + geometryVal.getSrid())
            .verifyComplete();
    }
}