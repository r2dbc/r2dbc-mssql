/*
 * Copyright 2018-2019 the original author or authors.
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
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.message.type.TypeUtils;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * Codec for temporal types that are represented as {@link LocalDateTime}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#SMALLDATETIME}, {@link SqlServerType#DATETIME}, and {@link SqlServerType#DATETIME2}</li>
 * <li>Java type: {@link LocalDateTime}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class LocalDateTimeCodec extends AbstractCodec<LocalDateTime> {

    /**
     * Singleton instance.
     */
    public static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();

    /**
     * Date-Time base date: 1900-01-01T00:00:00.0.
     */
    private static final LocalDateTime DATETIME_ZERO = LocalDateTime.of(1900, 1, 1, 0, 0, 0, 0);

    private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

    private LocalDateTimeCodec() {
        super(LocalDateTime.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, LocalDateTime value) {

        return RpcEncoding.encode(allocator, SqlServerType.DATETIME2, 8, value,
            (buffer, localDateTime) -> {
                encode(buffer, SqlServerType.DATETIME2, TypeUtils.MAX_FRACTIONAL_SECONDS_SCALE, localDateTime);
            });
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.encodeTemporalNull(allocator, SqlServerType.DATETIME2, 7);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType() == SqlServerType.SMALLDATETIME || typeInformation.getServerType() == SqlServerType.DATETIME || typeInformation.getServerType() == SqlServerType.DATETIME2;
    }

    @Override
    LocalDateTime doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends LocalDateTime> valueType) {

        if (length.isNull()) {
            return null;
        }

        if (type.getServerType() == SqlServerType.SMALLDATETIME) {

            int daysSinceBaseDate = Decode.uShort(buffer);
            int minutesSinceMidnight = Decode.uShort(buffer);

            return DATETIME_ZERO.plusDays(daysSinceBaseDate).plusMinutes(minutesSinceMidnight);
        }

        if (type.getServerType() == SqlServerType.DATETIME) {

            int daysSinceBaseDate = Decode.asInt(buffer);

            long ticksSinceMidnight = (Decode.asInt(buffer) * 10 + 1) / 3;
            int subSecondNanos = (int) ((ticksSinceMidnight * NANOS_PER_MILLISECOND) % NANOS_PER_SECOND);

            return DATETIME_ZERO.plusDays(daysSinceBaseDate).plus(ticksSinceMidnight, ChronoUnit.MILLIS).withNano(subSecondNanos);
        }

        if (type.getServerType() == SqlServerType.DATETIME2) {

            LocalTime localTime = LocalTimeCodec.INSTANCE.doDecode(buffer, length, type, LocalTime.class);
            LocalDate localDate = LocalDateCodec.INSTANCE.doDecode(buffer, length, type, LocalDate.class);

            return localTime.atDate(localDate);
        }

        throw new UnsupportedOperationException(String.format("Cannot decode value from server type [%s]", type.getServerType()));
    }

    static void encode(ByteBuf buffer, SqlServerType type, int scale, LocalDateTime value) {

        if (type == SqlServerType.SMALLDATETIME) {

            LocalDateTime midnight = value.truncatedTo(ChronoUnit.DAYS);
            int daysSinceBaseDate = Math.toIntExact(Duration.between(DATETIME_ZERO, midnight).toDays());
            int minutesSinceMidnight = (int) Duration.between(midnight, value).toMinutes();

            Encode.uShort(buffer, daysSinceBaseDate);
            Encode.uShort(buffer, minutesSinceMidnight);

            return;
        }

        if (type == SqlServerType.DATETIME) {

            LocalDateTime midnight = value.truncatedTo(ChronoUnit.DAYS);
            int daysSinceBaseDate = Math.toIntExact(Duration.between(DATETIME_ZERO, midnight).toDays());

            Duration time = Duration.between(midnight, value);

            long ticksSinceMidnight = (3 * time.toMillis() + 5) / 10;

            Encode.asInt(buffer, daysSinceBaseDate);
            Encode.asInt(buffer, (int) ticksSinceMidnight);

            return;
        }

        if (type == SqlServerType.DATETIME2) {

            LocalTimeCodec.doEncode(buffer, scale, value.toLocalTime());
            LocalDateCodec.encode(buffer, value.toLocalDate());

            return;
        }

        throw new UnsupportedOperationException(String.format("Cannot encode [%s] to server type [%s]", value, type));
    }
}
