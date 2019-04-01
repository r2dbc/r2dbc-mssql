/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.mssql.message.type;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.ProtocolException;

/**
 * Descriptor for PLP data length in row results.
 * This class encapsulates the total length of a value using a 8 byte unsigned long counter.
 * Use {@link Length} to encode/decode length headers of a PLP chunk and {@link PlpLength} to
 * encode/decode the the total PLP stream length
 *
 * @author Mark Paluch
 * @see Length
 */
public final class PlpLength {

    public static final long PLP_NULL = 0xFFFFFFFFFFFFFFFFL;

    public static final long UNKNOWN_PLP_LEN = 0xFFFFFFFFFFFFFFFEL;

    private final long length;

    private final boolean isNull;

    private PlpLength(long length, boolean isNull) {
        this.length = length;
        this.isNull = isNull;
    }

    /**
     * Creates a {@link PlpLength} that indicates the value is {@code null}.
     *
     * @return a {@link PlpLength} for {@code null}.
     */
    public static PlpLength nullLength() {
        return of(0, true);
    }

    /**
     * Creates a {@link PlpLength} with a given {@code length}.
     *
     * @return a {@link PlpLength} for a non-{@code null} value of the given {@code length}.
     */
    public static PlpLength of(int length) {
        return of(length, false);
    }

    /**
     * Creates a {@link PlpLength}.
     *
     * @param length value length.
     * @param isNull {@literal true} if the value is {@code null}.
     * @return the {@link PlpLength}.
     */
    public static PlpLength of(long length, boolean isNull) {
        return new PlpLength(length, isNull);
    }

    /**
     * Decode a {@link PlpLength} for a {@link TypeInformation}.
     *
     * @param buffer the data buffer.
     * @param type   {@link TypeInformation}.
     * @return the {@link PlpLength}.
     */
    public static PlpLength decode(ByteBuf buffer, TypeInformation type) {

        if (type.getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            long length = Decode.uLongLong(buffer);
            return PlpLength.of(length == PLP_NULL ? 0 : length, length == PLP_NULL);
        }


        throw ProtocolException.invalidTds("Cannot parse using " + type.getLengthStrategy());
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an {@link PlpLength}.
     *
     * @param buffer the data buffer.
     * @param type   {@link TypeInformation}.
     * @return {@literal true} if the buffer contains sufficient data to decode a {@link PlpLength}.
     */
    public static boolean canDecode(ByteBuf buffer, TypeInformation type) {

        int readerIndex = buffer.readerIndex();
        try {
            return doCanDecode(buffer, type);
        } finally {
            buffer.readerIndex(readerIndex);
        }
    }

    /**
     * Encode length or PLP_NULL.
     *
     * @param buffer the data buffer.
     */
    public void encode(ByteBuf buffer) {

        if (isNull()) {
            Encode.uLongLong(buffer, PLP_NULL);
        } else {
            Encode.uLongLong(buffer, getLength());
        }
    }

    private static boolean doCanDecode(ByteBuf buffer, TypeInformation type) {

        if (type.getLengthStrategy() == LengthStrategy.PARTLENTYPE) {
            return buffer.readableBytes() >= 8;
        }

        throw ProtocolException.invalidTds("Cannot parse value LengthDescriptor");
    }

    public long getLength() {
        return this.length;
    }

    public boolean isNull() {
        return this.isNull;
    }

    public boolean isUnknown() {
        return this.length == UNKNOWN_PLP_LEN;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [length=").append(this.length);
        sb.append(", isNull=").append(this.isNull);
        sb.append(']');
        return sb.toString();
    }
}
