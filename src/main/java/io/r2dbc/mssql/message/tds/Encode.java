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

package io.r2dbc.mssql.message.tds;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

/**
 * Encode utilities for TDS. Encoding methods typically accept {@link ByteBuffer} and a value and write the encoded value to the given buffer.
 *
 * @author Mark Paluch
 */
public final class Encode {

    public static final int U_SHORT_MAX_VALUE = Math.abs(Short.MIN_VALUE) + Short.MAX_VALUE;

    private Encode() {
    }

    /**
     * Encode a byte. SQL server type {@code BYTE}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void asByte(ByteBuf buffer, int value) {
        buffer.writeByte(value);
    }

    /**
     * Encode a byte. SQL server type {@code BYTE}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void asByte(ByteBuf buffer, byte value) {
        buffer.writeByte(value);
    }

    /**
     * Encode a float. SQL server type {@code REAL}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void asFloat(ByteBuf buffer, float value) {
        buffer.writeIntLE(Float.floatToIntBits(value));
    }

    /**
     * Encode a double. SQL server type {@code FLOAT}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void asDouble(ByteBuf buffer, double value) {
        buffer.writeLongLE(Double.doubleToLongBits(value));
    }

    /**
     * Encode a double word. SQL server type {@code DWORD}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void dword(ByteBuf buffer, int value) {
        buffer.writeIntLE(value);
    }

    /**
     * Encode long number. SQL server type {@code LONG}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void asLong(ByteBuf buffer, int value) {
        buffer.writeIntLE(value);
    }

    /**
     * Encode an unscaled {@link BigInteger} value. SQL server type {@code SMALLMONEY}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void smallMoney(ByteBuf buffer, BigInteger value) {
        buffer.writeIntLE(value.intValue());
    }

    /**
     * Encode an unscaled {@link BigInteger} value. SQL server type {@code MONEY}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void money(ByteBuf buffer, BigInteger value) {

        int intBitsHi = (int) (value.longValue() >> 32 & 0xFFFFFFFFL);
        int intBitsLo = (int) (value.longValue() & 0xFFFFFFFFL);

        buffer.writeIntLE(intBitsHi);
        buffer.writeIntLE(intBitsLo);
    }

    /**
     * Encode a bit. SQL server type {@code BIT}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void bit(ByteBuf buffer, boolean value) {
        asByte(buffer, (byte) (value ? 1 : 0));
    }

    /**
     * Encode byte number. SQL server type {@code TINYINT}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void tinyInt(ByteBuf buffer, byte value) {
        asByte(buffer, value);
    }

    /**
     * Encode short number. SQL server type {@code SMALLINT}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void smallInt(ByteBuf buffer, short value) {
        buffer.writeShortLE(value);
    }

    /**
     * Encode integer number. SQL server type {@code INT}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void asInt(ByteBuf buffer, int value) {
        buffer.writeIntLE(value);
    }

    /**
     * Encode long number. SQL server type {@code BIGINT}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void bigint(ByteBuf buffer, long value) {
        buffer.writeLongLE(value);
    }

    /**
     * Encode unsigned long number. SQL server type {@code LONGLONG}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void uLongLong(ByteBuf buffer, long value) {
        buffer.writeLongLE(value);
    }

    /**
     * Encode a unsigned short. SQL server type {@code USHORT}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void uShort(ByteBuf buffer, int value) {

        if (value > U_SHORT_MAX_VALUE) {
            throw new IllegalArgumentException("Value " + value + " exceeds uShort.MAX_VALUE");
        }

        buffer.writeShortLE(value);
    }

    /**
     * Encode an integer with big endian encoding. Typically used to write bit masks.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void intBigEndian(ByteBuf buffer, int value) {
        buffer.writeInt(value);
    }

    /**
     * Encode a short big endian.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void shortBE(ByteBuf buffer, short value) {
        buffer.writeShort(value);
    }

    /**
     * Encode a short (ushort) big endian.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void uShortBE(ByteBuf buffer, int value) {

        if (value > U_SHORT_MAX_VALUE) {
            throw new IllegalArgumentException("Value " + value + " exceeds uShort.MAX_VALUE");
        }

        buffer.writeShort(value);
    }

    /**
     * Encode a string. SQL server type {@code VARCHAR}/{@code NVARCHAR}.
     *
     * @param buffer  the data buffer.
     * @param value   the value to encode.
     * @param charset the charset to use.
     */
    public static void uString(ByteBuf buffer, String value, Charset charset) {

        ByteBuf encoded = ByteBufUtil.encodeString(buffer.alloc(), CharBuffer.wrap(value), charset);
        uShort(buffer, encoded.readableBytes());
        buffer.writeBytes(encoded);
        encoded.release();
    }

    /**
     * Encode a {@link String} as unicode. SQL server type {@code UNICODESTREAM}.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void unicodeStream(ByteBuf buffer, String value) {

        ByteBuf encoded = ByteBufUtil.encodeString(buffer.alloc(), CharBuffer.wrap(value), ServerCharset.UNICODE.charset());

        buffer.writeBytes(encoded);

        encoded.release();
    }

    /**
     * Encode a {@link String} as RPC string.
     *
     * @param buffer the data buffer.
     * @param value  the value to encode.
     */
    public static void rpcString(ByteBuf buffer, CharSequence value) {

        for (int i = 0; i < value.length(); i++) {

            char ch = value.charAt(i);
            buffer.writeByte((byte) (ch & 0xFF));
            buffer.writeByte((byte) ((ch >> 8) & 0xFF));
        }
    }
}
