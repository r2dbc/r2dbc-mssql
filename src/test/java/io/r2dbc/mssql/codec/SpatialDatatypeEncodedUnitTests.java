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

import com.microsoft.sqlserver.jdbc.Geometry;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.token.RpcRequest;
import io.r2dbc.mssql.message.type.*;
import io.r2dbc.mssql.util.EncodedAssert;
import io.r2dbc.mssql.util.TdsEncoded;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import static io.r2dbc.mssql.message.type.TypeInformation.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SpatialDatatypeEncoded} and {@link SpatialDatatypePlpEncoded}.
 */
class SpatialDatatypeEncodedUnitTests {

    static final TypeInformation GEOMETRY = builder().withLengthStrategy(LengthStrategy.PARTLENTYPE).withServerType(SqlServerType.GEOMETRY).build();

    @Test
    void shouldEncodeInlineUpToShortVartypeLimit() {

        byte[] value = new byte[TypeUtils.SHORT_VARTYPE_MAX_BYTES];

        Encoded encoded = SpatialDatatypeEncoded.encode(TestByteBufAllocator.TEST, SqlServerType.GEOMETRY, value);

        assertThat(encoded).isExactlyInstanceOf(SpatialDatatypeEncoded.class);

        EncodedAssert.assertThat(encoded).isEncodedAs(expected -> {
            SpatialDatatypeEncoded.encodeTypeInformation(expected, SqlServerType.GEOMETRY);
            PlpLength.of(value.length).encode(expected);
            Length.of(value.length).encode(expected, LengthStrategy.PARTLENTYPE);
            expected.writeBytes(value);
            Length.of(0).encode(expected, LengthStrategy.PARTLENTYPE);
        });
    }

    @Test
    void shouldEncodePlpStreamAboveShortVartypeLimit() {

        byte[] value = new byte[TypeUtils.SHORT_VARTYPE_MAX_BYTES + 1];

        Encoded encoded = SpatialDatatypeEncoded.encode(TestByteBufAllocator.TEST, SqlServerType.GEOMETRY, value);

        assertThat(encoded).isExactlyInstanceOf(SpatialDatatypePlpEncoded.class);
        assertThat(encoded.getDataType()).isEqualTo(TdsDataType.UDT);
        assertThat(encoded.getFormalType()).isEqualTo("geometry");
    }

    @Test
    void shouldStreamLargeSpatialValueWithPlpTerminator() throws SQLServerException {

        Geometry geometry = Geometry.STGeomFromText(largeLineString(), 0);
        byte[] value = geometry.serialize();
        assertThat(value.length).isGreaterThan(TypeUtils.SHORT_VARTYPE_MAX_BYTES);

        Encoded encoded = GeometryCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), geometry);
        assertThat(encoded).isExactlyInstanceOf(SpatialDatatypePlpEncoded.class);

        RpcRequest request = RpcRequest.builder()
                .withProcId(RpcRequest.Sp_ExecuteSql)
                .withTransactionDescriptor(TransactionDescriptor.empty())
                .withParameter(RpcDirection.IN, encoded)
                .build();

        ByteBuf wire = TestByteBufAllocator.TEST.buffer();
        Flux.from(request.encode(TestByteBufAllocator.TEST, 1024))
                .doOnNext(fragment -> {
                    wire.writeBytes(fragment.getByteBuf());
                    fragment.getByteBuf().release();
                })
                .blockLast();

        wire.skipBytes(wire.readIntLE() - 4); // ALL_HEADERS (leading DWORD includes itself)
        assertThat(Decode.uShort(wire)).isEqualTo(0xFFFF); // proc id switch
        Decode.uShort(wire); // proc id
        Decode.uShort(wire); // option flags

        assertThat(Decode.asByte(wire)).isEqualTo((byte) 0); // parameter name (empty)
        Decode.asByte(wire); // status flags
        assertThat(Decode.asByte(wire)).isEqualTo(TdsDataType.UDT.getValue());

        assertThat(Decode.unicodeBString(wire)).isEqualTo("master");
        assertThat(Decode.unicodeBString(wire)).isEqualTo("sys");
        assertThat(Decode.unicodeBString(wire)).isEqualTo("geometry");

        assertThat(PlpLength.decode(wire, GEOMETRY).isUnknown()).isTrue();

        ByteBuf payload = TestByteBufAllocator.TEST.buffer();
        int chunkLength;
        do {
            chunkLength = Length.decode(wire, GEOMETRY).getLength();
            payload.writeBytes(wire, chunkLength);
        } while (chunkLength != 0);

        assertThat(wire.readableBytes()).describedAs("Bytes after zero-length PLP terminator").isEqualTo(0);

        byte[] streamed = new byte[payload.readableBytes()];
        payload.readBytes(streamed);
        assertThat(streamed).isEqualTo(value);

        Geometry roundTripped = GeometryCodec.INSTANCE.decode(TdsEncoded.plpStream(GEOMETRY, streamed), ColumnUtil.createColumn(GEOMETRY), Geometry.class);
        assertThat(roundTripped.STAsText()).isEqualTo(geometry.STAsText());

        wire.release();
        payload.release();
    }

    private static String largeLineString() {

        StringBuilder wkt = new StringBuilder("LINESTRING(");

        for (int i = 0; i < 600; i++) {

            if (i > 0) {
                wkt.append(", ");
            }
            wkt.append(i).append(" ").append(i % 100);
        }

        return wkt.append(")").toString();
    }

}
