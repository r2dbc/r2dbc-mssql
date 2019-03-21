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
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;

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

    /**
     * Peek onto the next {@link #uShort(ByteBuf)}. This method retains the {@link ByteBuf#readerIndex()} and returns the {@code USHORT} value if it is readable (i.e. if the buffer has at least two
     * readable bytes). Returns {@code null} if not readable.
     *
     * @param buffer the data buffer.
     * @return the peeked {@code USHORT} value or {@code null}.
     */
    @Nullable
    public static Integer peekUShort(ByteBuf buffer) {

        if (buffer.readableBytes() >= 2) {

            buffer.markReaderIndex();
            int peek = Decode.uShort(buffer);
            buffer.resetReaderIndex();

            return peek;
        }

        return null;
    }

    /**
     * Read an integer with big endian encoding. Typically used to evaluate bit masks.
     *
     * @param buffer the data buffer.
     * @return
     */
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
        return as(buffer, buffer.readUnsignedShortLE() * 2, ServerCharset.UNICODE.charset());
    }

    /**
     * Decode a unicode ({@code VARCHAR}) string from {@link ByteBuf} with {@code byte} length.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static String unicodeBString(ByteBuf buffer) {
        return as(buffer, buffer.readByte() * 2, ServerCharset.UNICODE.charset());
    }

    /**
     * Decode the {@link ByteBuf} using the given {@link Charset}.
     *
     * @param buffer  the data buffer.
     * @param length
     * @param charset
     * @return
     */
    private static String as(ByteBuf buffer, int length, Charset charset) {

        Assert.requireNonNull(buffer, "Buffer must not be null");
        Assert.requireNonNull(charset, "Charset must not be null");

        String result = buffer.toString(buffer.readerIndex(), length, charset);
        buffer.skipBytes(length);
        return result;
    }
}
