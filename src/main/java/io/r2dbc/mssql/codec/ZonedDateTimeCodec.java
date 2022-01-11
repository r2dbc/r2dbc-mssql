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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

/**
 * Codec for temporal types that are represented as {@link ZonedDateTime}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#DATETIMEOFFSET}</li>
 * <li>Java type: {@link ZonedDateTime}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class ZonedDateTimeCodec extends AbstractCodec<ZonedDateTime> {

    /**
     * Singleton instance.
     */
    static final ZonedDateTimeCodec INSTANCE = new ZonedDateTimeCodec();

    private static final byte[] NULL = ByteArray.fromEncoded((alloc) -> RpcEncoding.encodeTemporalNull(alloc, SqlServerType.DATETIMEOFFSET, 7));

    private ZonedDateTimeCodec() {
        super(ZonedDateTime.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, ZonedDateTime value) {
        return OffsetDateTimeCodec.INSTANCE.encode(allocator, context, value.toOffsetDateTime());
    }

    @Override
    public boolean canEncodeNull(SqlServerType serverType) {
        return serverType == SqlServerType.DATETIMEOFFSET;
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.wrap(NULL, SqlServerType.DATETIMEOFFSET);
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator, SqlServerType serverType) {
        return RpcEncoding.encodeTemporalNull(allocator, serverType, 7);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType() == SqlServerType.DATETIMEOFFSET;
    }

    @Override
    ZonedDateTime doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends ZonedDateTime> valueType) {

        if (length.isNull()) {
            return null;
        }

        OffsetDateTime offsetDateTime = OffsetDateTimeCodec.INSTANCE.doDecode(buffer, length, type, OffsetDateTime.class);
        return offsetDateTime.toZonedDateTime();
    }

}
