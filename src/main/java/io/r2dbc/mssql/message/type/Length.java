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

package io.r2dbc.mssql.message.type;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.ProtocolException;

/**
 * Descriptor for data length in row results.
 *
 * @author Mark Paluch
 */
public final class Length {

    public static final long PLP_NULL = 0xFFFFFFFFFFFFFFFFL;

    public static final int USHORT_NULL = 65535;

    public static final int UNKNOWN_STREAM_LENGTH = -1;

    private final int length;

    private final boolean isNull;

    private Length(int length, boolean isNull) {
        this.length = length;
        this.isNull = isNull;
    }

    /**
     * Creates a {@link Length} that indicates the value is {@literal null}.
     *
     * @return a {@link Length} for {@literal null}.
     */
    public static Length nullLength() {
        return new Length(0, true);
    }

    /**
     * Creates a {@link Length} with a given {@code length}.
     *
     * @return a {@link Length} for a non-{@literal null} value of the given {@code length}.
     */
    public static Length of(int length) {
        return new Length(length, false);
    }

    /**
     * Creates a {@link Length}.
     *
     * @param length value length.
     * @param isNull {@literal true} if the value is {@literal null}.
     * @return the {@link Length}.
     */
    public static Length of(int length, boolean isNull) {
        return new Length(length, isNull);
    }

    /**
     * Decode a {@link Length} for a {@link TypeInformation}.
     *
     * @param buffer the data buffer.
     * @param type   {@link TypeInformation}.
     * @return the {@link Length}.
     */
    public static Length decode(ByteBuf buffer, TypeInformation type) {

        switch (type.getLengthStrategy()) {

            case PARTLENTYPE: {
                buffer.markReaderIndex();
                long length = buffer.readLong();
                buffer.resetReaderIndex();

                return new Length(UNKNOWN_STREAM_LENGTH, length == PLP_NULL);
            }

            case FIXEDLENTYPE:
                return new Length(type.getMaxLength(), type.getMaxLength() == 0);

            case BYTELENTYPE: {
                int length = Decode.uByte(buffer);
                return new Length(length, length == 0);
            }

            case USHORTLENTYPE: {
                int length = Decode.uShort(buffer);
                return new Length(length == USHORT_NULL ? 0 : length, length == USHORT_NULL);
            }

            case LONGLENTYPE: {

                SqlServerType serverType = type.getServerType();

                if (serverType == SqlServerType.TEXT || serverType == SqlServerType.IMAGE
                    || serverType == SqlServerType.NTEXT) {

                    int nullMarker = Decode.uByte(buffer);

                    if (nullMarker == 0) {
                        return new Length(0, true);
                    }

                    // skip(24) is to skip the textptr and timestamp fields
                    buffer.skipBytes(24);
                    int valueLength = Decode.asLong(buffer);


                    return new Length(valueLength, false);
                }

                if (serverType == SqlServerType.SQL_VARIANT) {
                    int valueLength = Decode.intBigEndian(buffer);
                    return new Length(valueLength, valueLength == 0);
                }

                int length = Decode.uShort(buffer);
                return new Length(length == USHORT_NULL ? 0 : length, length == USHORT_NULL);
            }
        }

        throw ProtocolException.invalidTds("Cannot parse value LengthDescriptor");
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an {@link Length}.
     *
     * @param buffer the data buffer.
     * @param type   {@link TypeInformation}.
     * @return {@literal true} if the buffer contains sufficient data to decode a {@link Length}.
     */
    public static boolean canDecode(ByteBuf buffer, TypeInformation type) {

        int readerIndex = buffer.readerIndex();
        try {
            return doCanDecode(buffer, type);
        } finally {
            buffer.readerIndex(readerIndex);
        }
    }

    private static boolean doCanDecode(ByteBuf buffer, TypeInformation type) {

        switch (type.getLengthStrategy()) {

            case PARTLENTYPE:
                return buffer.readableBytes() >= 4;

            case FIXEDLENTYPE:
                return true;


            case BYTELENTYPE:
            case USHORTLENTYPE:
                return buffer.readableBytes() >= 2;

            case LONGLENTYPE: {

                SqlServerType serverType = type.getServerType();

                if (serverType == SqlServerType.TEXT || serverType == SqlServerType.IMAGE
                    || serverType == SqlServerType.NTEXT) {

                    if (buffer.readableBytes() == 0) {
                        return false;
                    }

                    int nullMarker = Decode.uByte(buffer);

                    if (nullMarker == 0) {
                        return true;
                    }


                    // skip(24) is to skip the textptr and timestamp fields
                    return buffer.readableBytes() >= 24 + /* int */ 4;
                }

                if (serverType == SqlServerType.SQL_VARIANT) {
                    return buffer.readableBytes() >= 4;
                }

                return buffer.readableBytes() >= 2;
            }
        }

        throw ProtocolException.invalidTds("Cannot parse value LengthDescriptor");
    }

    public void encode(ByteBuf buffer, TypeInformation type) {

        switch (type.getLengthStrategy()) {

            case PARTLENTYPE:

                buffer.writeLong(getLength());
                return;

            case FIXEDLENTYPE:
                return;

            case BYTELENTYPE:

                if (isNull()) {
                    Encode.asByte(buffer, (byte) 0);
                } else {
                    Encode.asByte(buffer, (byte) getLength());
                }
                return;

            case USHORTLENTYPE:

                if (isNull()) {
                    Encode.uShort(buffer, USHORT_NULL);
                } else {
                    Encode.uShort(buffer, getLength());
                }

                return;

            case LONGLENTYPE:

                SqlServerType serverType = type.getServerType();

                if (serverType == SqlServerType.TEXT || serverType == SqlServerType.IMAGE
                    || serverType == SqlServerType.NTEXT) {

                    if (isNull()) {
                        Encode.asByte(buffer, (byte) 0);
                        return;
                    }

                    // skip(24) is to skip the textptr and timestamp fields
                    buffer.skipBytes(24);
                    buffer.writeLong(getLength());
                    return;
                }

                if (serverType == SqlServerType.SQL_VARIANT) {

                    Encode.intBigEndian(buffer, getLength());
                    return;
                }

                if (isNull()) {
                    Encode.uShortBE(buffer, USHORT_NULL);
                } else {
                    Encode.uShortBE(buffer, getLength());
                }

                return;

        }

        throw ProtocolException.invalidTds("Cannot parse value LengthDescriptor");
    }

    public int getLength() {
        return this.length;
    }

    public boolean isNull() {
        return this.isNull;
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
