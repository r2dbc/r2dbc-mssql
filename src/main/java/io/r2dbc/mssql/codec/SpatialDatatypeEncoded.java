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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.*;
import reactor.core.publisher.Mono;

/**
 * @since 1.0.6
 */
final class SpatialDatatypeEncoded extends RpcEncoding.HintedEncoded {

    private static final String DATABASE_NAME = "master";

    private static final String SCHEMA_NAME = "sys";

    private SpatialDatatypeEncoded(ByteBufAllocator allocator, SqlServerType serverType, byte[] value) {
        super(TdsDataType.UDT, serverType, Encoded.ofLengthAware(estimateLength(serverType, value), ignored -> {

            ByteBuf buffer = allocator.buffer(estimateLength(serverType, value));
            encodeTypeInformation(buffer, serverType);

            if (value == null) {
                PlpLength.nullLength().encode(buffer);
            } else {
                PlpLength.of(value.length).encode(buffer);
                Length.of(value.length).encode(buffer, TdsDataType.UDT.getLengthStrategy());
                buffer.writeBytes(value);
                Length.of(0).encode(buffer, TdsDataType.UDT.getLengthStrategy());
            }

            return buffer;
        }));
    }

    static Encoded encode(ByteBufAllocator allocator, SqlServerType serverType, byte[] value) {

        if (value.length > TypeUtils.SHORT_VARTYPE_MAX_BYTES) {
            return new SpatialDatatypePlpEncoded(serverType, allocator, Mono.just(Unpooled.wrappedBuffer(value)), () -> {
            });
        }

        return new SpatialDatatypeEncoded(allocator, serverType, value);
    }

    static Encoded encodeNull(ByteBufAllocator allocator, SqlServerType serverType) {
        return new SpatialDatatypeEncoded(allocator, serverType, null);
    }

    static void encodeTypeInformation(ByteBuf buffer, SqlServerType serverType) {

        encodeUnicodeBString(buffer, DATABASE_NAME);
        encodeUnicodeBString(buffer, SCHEMA_NAME);
        encodeUnicodeBString(buffer, serverType.toString());
    }

    private static int estimateLength(SqlServerType serverType, byte[] value) {
        return unicodeBStringLength(DATABASE_NAME) + unicodeBStringLength(SCHEMA_NAME) + unicodeBStringLength(serverType.toString()) + 8
                + (value == null ? 0 : 4 + value.length + 4);
    }

    private static void encodeUnicodeBString(ByteBuf buffer, String value) {
        Encode.asByte(buffer, value.length());
        Encode.rpcString(buffer, value);
    }

    private static int unicodeBStringLength(String value) {
        return 1 + value.length() * 2;
    }

}
