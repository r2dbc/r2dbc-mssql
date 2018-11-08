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
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.TdsDataType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.message.type.TypeInformation.SqlServerType;
import io.r2dbc.mssql.message.type.TypeUtils;
import io.r2dbc.mssql.util.Assert;

import java.time.LocalTime;

/**
 * Codec for scaled time types that are represented as {@link LocalTime}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#TIME}</li>
 * <li>Java type: {@link LocalTime}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class LocalTimeCodec extends AbstractCodec<LocalTime> {

    /**
     * Singleton instance.
     */
    public static final LocalTimeCodec INSTANCE = new LocalTimeCodec();

    /**
     * Using known multipliers is faster than calculating these (10^n).
     */
    private static final int[] SCALED_MULTIPLIERS = new int[]{10000000, 1000000, 100000, 10000, 1000, 100, 10, 1};

    private LocalTimeCodec() {
        super(LocalTime.class);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType() == SqlServerType.DATE;
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.encodeTemporalNull(allocator, SqlServerType.TIME);
    }

    @Override
    LocalTime doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends LocalTime> valueType) {

        long nanosSinceMidnight = 0;
        int scale = type.getScale();

        Assert.isTrue(scale >= 0 && scale <= TypeUtils.MAX_FRACTIONAL_SECONDS_SCALE, "Invalid fractional scale");

        int valueLength = TypeUtils.getTimeValueLength(scale);
        for (int i = 0; i < valueLength; i++) {
            nanosSinceMidnight |= (buffer.readByte() & 0xFFL) << (8 * i);
        }
        nanosSinceMidnight *= SCALED_MULTIPLIERS[scale];

        return LocalTime.ofNanoOfDay(nanosSinceMidnight);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, LocalTime value) {
        return RpcEncoding.encode(allocator, TdsDataType.TIMEN, SqlServerType.TIME, TypeUtils.MAX_FRACTIONAL_SECONDS_SCALE, TypeUtils.getTimeValueLength(TypeUtils.MAX_FRACTIONAL_SECONDS_SCALE),
            value, (buffer,
                    localTime) -> doEncode(buffer,
                TypeUtils.MAX_FRACTIONAL_SECONDS_SCALE, localTime));
    }

    static void doEncode(ByteBuf buffer, int scale, LocalTime value) {

        int valueLength = TypeUtils.getTimeValueLength(scale);
        doEncodeValue(buffer, valueLength, value);
    }

    private static void doEncodeValue(ByteBuf buffer, int valueLength, LocalTime value) {

        long nanosSinceMidnight = value.toNanoOfDay();
        nanosSinceMidnight /= SCALED_MULTIPLIERS[valueLength];

        for (int i = 0; i < valueLength; i++) {
            buffer.writeByte((byte) ((nanosSinceMidnight >> (8 * i)) & 0xFF));
        }
    }
}
