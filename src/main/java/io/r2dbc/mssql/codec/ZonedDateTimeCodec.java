/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import io.r2dbc.mssql.message.type.TdsDataType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.message.type.TypeInformation.SqlServerType;
import io.r2dbc.mssql.message.type.TypeUtils;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
    public static final ZonedDateTimeCodec INSTANCE = new ZonedDateTimeCodec();

    private ZonedDateTimeCodec() {
        super(ZonedDateTime.class);
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
        
        LocalTime localTime = LocalTimeCodec.INSTANCE.doDecode(buffer, length, type, LocalTime.class);
        LocalDate localDate = LocalDateCodec.INSTANCE.doDecode(buffer, length, type, LocalDate.class);

        int localMinutesOffset = Decode.uShort(buffer);
        ZoneOffset offset = ZoneOffset.ofTotalSeconds(localMinutesOffset * 60);

        return ZonedDateTime.ofStrict(localTime.atDate(localDate), offset, ZoneId.ofOffset("UT", offset)).plusMinutes(localMinutesOffset);
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.encodeTemporalNull(allocator, SqlServerType.DATETIMEOFFSET, 7);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, ZonedDateTime value) {

        ByteBuf buffer = allocator.buffer(12);

        Encode.asByte(buffer, 7); // scale
        Encode.asByte(buffer, 0x0a); // length

        doEncode(buffer, value.minusSeconds(value.getOffset().getTotalSeconds()));

        return new RpcEncoding.HintedEncoded(TdsDataType.DATETIMEOFFSETN, SqlServerType.DATETIMEOFFSET, buffer);
    }

    static void doEncode(ByteBuf buffer, ZonedDateTime value) {

        LocalTimeCodec.doEncode(buffer, TypeUtils.MAX_FRACTIONAL_SECONDS_SCALE, value.toLocalTime());
        LocalDateCodec.doEncode(buffer, value.toLocalDate());

        int localMinutesOffset = value.getOffset().getTotalSeconds() / 60;

        Encode.uShort(buffer, localMinutesOffset);
    }
}
