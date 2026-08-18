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
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;

/**
 * @since 1.0.6
 */
final class SpatialDatatypePlpEncoded extends PlpEncoded {

    private final SqlServerType serverType;

    SpatialDatatypePlpEncoded(SqlServerType serverType, ByteBufAllocator allocator, Publisher<ByteBuf> dataStream, Disposable disposable) {
        super(SqlServerType.VARBINARYMAX, allocator, dataStream, disposable);
        this.serverType = serverType;
    }

    @Override
    public TdsDataType getDataType() {
        return TdsDataType.UDT;
    }

    @Override
    public void encodeHeader(ByteBuf byteBuf) {
        SpatialDatatypeEncoded.encodeTypeInformation(byteBuf, this.serverType);
    }

    @Override
    public String getFormalType() {
        return this.serverType.toString();
    }

}
