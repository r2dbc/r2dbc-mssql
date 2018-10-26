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

package io.r2dbc.mssql.message.tds;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.type.Encoding;

import java.util.Objects;

/**
 * TDS-specific decode methods. This utility provides decoding methods according to TDS types.
 *
 * @author Mark Paluch
 */
public final class Decode {

    private Decode() {
    }

    /**
     * Decode a byte. SQL server type {@code BYTE}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static byte asByte(ByteBuf buffer) {
        return buffer.readByte();
    }


    /**
     * Decode an unsigned byte. SQL server type {@code BYTE}
     *
     * @param buffer the data buffer.
     * @return
     */
    public static int uByte(ByteBuf buffer) {
        return buffer.readUnsignedByte();
    }

    /**
     * Decode a double word. SQL server type {@code DWORD}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static long dword(ByteBuf buffer) {
        return buffer.readUnsignedIntLE();
    }

    /**
     * Decode byte number. SQL server type {@code BIT}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static byte bit(ByteBuf buffer) {
        return asByte(buffer);
    }

    /**
     * Decode float number. SQL server type {@code REAL}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static float asFloat(ByteBuf buffer) {
        return Float.intBitsToFloat(buffer.readIntLE());
    }

    /**
     * Decode double number. SQL server type {@code FLOAT}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static double asDouble(ByteBuf buffer) {
        return Double.longBitsToDouble(buffer.readLongLE());
    }

    /**
     * Decode byte number. SQL server type {@code TINYINT}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static byte tinyInt(ByteBuf buffer) {
        return asByte(buffer);
    }

    /**
     * Decode short number. SQL server type {@code SMALLINT}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static short smallInt(ByteBuf buffer) {
        return buffer.readShortLE();
    }

    /**
     * Decode integer number. SQL server type {@code INT}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static int asInt(ByteBuf buffer) {
        return buffer.readIntLE();
    }

    /**
     * Decode long number. SQL server type {@code BIGINT}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static long bigint(ByteBuf buffer) {
        return buffer.readLongLE();
    }

    /**
     * Decode long number. SQL server type {@code LONG}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static int asLong(ByteBuf buffer) {
        return buffer.readIntLE();
    }

    /**
     * Decode unsigned long number. SQL server type {@code LONGLONG}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static long uLongLong(ByteBuf buffer) {
        return buffer.readLongLE();
    }

    /**
     * Decode a unsigned short. SQL server type {@code USHORT}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static int uShort(ByteBuf buffer) {
        return buffer.readUnsignedShortLE();
    }

    public static int intBigEndian(ByteBuf buffer) {
        return buffer.readInt();
    }

    /**
     * Decode a unicode ({@code VARCHAR}) string from {@link ByteBuf} with {@code unsigned short} length.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static String unicodeUString(ByteBuf buffer) {
        return as(buffer, buffer.readUnsignedShortLE() * 2, Encoding.UNICODE);
    }

    /**
     * Decode a unicode ({@code VARCHAR}) string from {@link ByteBuf} with {@code byte} length.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static String unicodeBString(ByteBuf buffer) {
        return as(buffer, buffer.readByte() * 2, Encoding.UNICODE);
    }

    /**
     * Decode a unicode ({@code VARCHAR}) string from {@link ByteBuf} with the given {@code length}).
     *
     * @param buffer the data buffer.
     * @param length length of bytes to read.
     * @return
     */
    public static String unicodeString(ByteBuf buffer, int length) {
        return as(buffer, length, Encoding.UNICODE);
    }

    /**
     * Decode the {@link ByteBuf} using the given {@link Encoding}.
     *
     * @param buffer   the data buffer.
     * @param length
     * @param encoding
     * @return
     */
    public static String as(ByteBuf buffer, int length, Encoding encoding) {

        Objects.requireNonNull(buffer, "Buffer must not be null");
        Objects.requireNonNull(encoding, "Encoding must not be null");

        String result = buffer.toString(buffer.readerIndex(), length, encoding.charset());
        buffer.skipBytes(length);
        return result;
    }
}
