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
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;
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
 * @author svats0001
 */
final class GeographyCodec extends AbstractCodec<Geography> {

    /**
     * Singleton instance.
     */
    static final GeographyCodec INSTANCE = new GeographyCodec();
    
    private GeographyCodec() {
        super(Geography.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Geography value) {
        return BinaryCodec.INSTANCE.encode(allocator, context, value.serialize());
    }

    @Override
    public boolean canEncodeNull(SqlServerType serverType) {
        return serverType == SqlServerType.GEOGRAPHY;
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator, SqlServerType serverType) {
        return BinaryCodec.INSTANCE.encodeNull(allocator, serverType);
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return BinaryCodec.INSTANCE.encodeNull(allocator);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType().equals(SqlServerType.GEOGRAPHY);
    }

    @Nullable
    public Geography decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends Geography> type) {

        Assert.requireNonNull(decodable, "Decodable must not be null");
        Assert.requireNonNull(type, "Type must not be null");

        if (buffer == null) {
            return null;
        }

        Length length;

        if (decodable.getType().getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            PlpLength plpLength = PlpLength.decode(buffer, decodable.getType());
            length = Length.of(Math.toIntExact(plpLength.getLength()), plpLength.isNull());
        } else {
            length = Length.decode(buffer, decodable.getType());
        }

        if (length.isNull()) {
            return null;
        }

        return doDecode(buffer, length, decodable.getType(), type);
    }

    @Override
    Geography doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends Geography> valueType) {

        if (length.isNull()) {
            return null;
        }

        byte[] geographyBytes = new byte[length.getLength()];

        if (type.getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            int dstIndex = 0;
            while (buffer.isReadable()) {
                int chunkLength = Length.decode(buffer, type).getLength();
                buffer.readBytes(geographyBytes, dstIndex, chunkLength);
                dstIndex += chunkLength;
            }

            try {
                return Geography.deserialize(geographyBytes);
            } catch (SQLServerException exc) {
                return null;
            }
        }

        buffer.readBytes(geographyBytes);
        try {
            return Geography.deserialize(geographyBytes);
        } catch (SQLServerException exc) {
            return null;
        }
    }
}