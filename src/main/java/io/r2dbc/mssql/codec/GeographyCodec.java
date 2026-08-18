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
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;

import reactor.util.annotation.Nullable;

/**
 * Codec for date types that are represented as {@link Geography}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#GEOGRAPHY}</li>
 * <li>Java type: {@link Geography}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @since 1.0.6
 */
final class GeographyCodec extends GeospatialCodecSupport<Geography> {

    /**
     * Singleton instance.
     */
    static final GeographyCodec INSTANCE = new GeographyCodec();

    private GeographyCodec() {
        super(Geography.class, SqlServerType.GEOGRAPHY);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Geography value) {
        return SpatialDatatypeEncoded.encode(allocator, SqlServerType.GEOGRAPHY, value.serialize());
    }

    @Override
    @Nullable
    Geography doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends Geography> valueType) {

        if (length.isNull()) {
            return null;
        }

        try {
            byte[] geographyBytes = Decode.readBytesOrPlp(buffer, length, type);
            return Geography.deserialize(geographyBytes);
        } catch (SQLServerException exc) {
            throw new SpatialDatatypeDecodeException("Cannot decode geography data", exc);
        }
    }

}
