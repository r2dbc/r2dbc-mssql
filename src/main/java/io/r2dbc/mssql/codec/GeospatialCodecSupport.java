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
import io.r2dbc.mssql.message.type.*;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

/**
 * @author Mark Paluch
 * @since 1.0.6
 */
abstract class GeospatialCodecSupport<T> extends AbstractCodec<T> {

    private final SqlServerType serverType;

    public GeospatialCodecSupport(Class<T> type, SqlServerType serverType) {
        super(type);
        this.serverType = serverType;
    }

    @Override
    public boolean canEncodeNull(SqlServerType serverType) {
        return this.serverType == serverType;
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator, SqlServerType serverType) {
        return SpatialDatatypeEncoded.encodeNull(allocator, this.serverType);
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return SpatialDatatypeEncoded.encodeNull(allocator, this.serverType);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType().equals(this.serverType);
    }

    @Nullable
    public T decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends T> type) {

        Assert.requireNonNull(decodable, "Decodable must not be null");
        Assert.requireNonNull(type, "Type must not be null");

        if (buffer == null) {
            return null;
        }

        Length length;

        if (decodable.getType().getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            PlpLength plpLength = PlpLength.decode(buffer, decodable.getType());
            length = Length.of(plpLength);
        } else {
            length = Length.decode(buffer, decodable.getType());
        }

        if (length.isNull()) {
            return null;
        }

        return doDecode(buffer, length, decodable.getType(), type);
    }

}
