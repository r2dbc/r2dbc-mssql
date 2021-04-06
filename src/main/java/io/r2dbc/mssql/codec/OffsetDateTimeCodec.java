/*
 * Copyright 2019-2021 the original author or authors.
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
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.message.type.TypeUtils;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Codec for temporal types that are represented as {@link OffsetDateTime}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#DATETIMEOFFSET}</li>
 * <li>Java type: {@link OffsetDateTime}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class OffsetDateTimeCodec extends AbstractCodec<OffsetDateTime> {

    /**
     * Singleton instance.
     */
    public static final OffsetDateTimeCodec INSTANCE = new OffsetDateTimeCodec();

    private static final byte[] NULL = ByteArray.fromEncoded((alloc) -> RpcEncoding.encodeTemporalNull(alloc, SqlServerType.DATETIMEOFFSET, 7));

    private OffsetDateTimeCodec() {
        super(OffsetDateTime.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, OffsetDateTime value) {

        ByteBuf buffer = allocator.buffer(12);

        Encode.asByte(buffer, 7); // scale
        Encode.asByte(buffer, 0x0a); // length

        doEncode(buffer, value.minusSeconds(value.getOffset().getTotalSeconds()));

        return new RpcEncoding.HintedEncoded(TdsDataType.DATETIMEOFFSETN, SqlServerType.DATETIMEOFFSET, buffer);
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.wrap(NULL, SqlServerType.DATETIMEOFFSET);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType() == SqlServerType.DATETIMEOFFSET;
    }

    @Override
    OffsetDateTime doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends OffsetDateTime> valueType) {

        if (length.isNull()) {
            return null;
        }

        LocalTime localTime = LocalTimeCodec.INSTANCE.doDecode(buffer, length, type, LocalTime.class);
        LocalDate localDate = LocalDateCodec.INSTANCE.doDecode(buffer, length, type, LocalDate.class);

        int localMinutesOffset = Decode.uShort(buffer);
        ZoneOffset offset = ZoneOffset.ofTotalSeconds(localMinutesOffset * 60);

        return OffsetDateTime.of(localTime.atDate(localDate), offset).plusMinutes(localMinutesOffset);
    }

    static void doEncode(ByteBuf buffer, OffsetDateTime value) {

        LocalTimeCodec.doEncode(buffer, TypeUtils.MAX_FRACTIONAL_SECONDS_SCALE, value.toLocalTime());
        LocalDateCodec.encode(buffer, value.toLocalDate());

        int localMinutesOffset = value.getOffset().getTotalSeconds() / 60;

        Encode.uShort(buffer, localMinutesOffset);
    }

}
