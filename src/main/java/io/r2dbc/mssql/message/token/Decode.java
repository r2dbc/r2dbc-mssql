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
package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.Encoding;

import java.util.Objects;

/**
 * @author Mark Paluch
 */
class Decode {

	private Decode() {}

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
	 * Decode a double word. SQL server type {@code DWORD}.
	 * 
	 * @param buffer the data buffer.
	 * @return
	 */
	public static long dword(ByteBuf buffer) {
		return buffer.readUnsignedIntLE();
	}

	/**
	 * Decode long number. SQL server type {@code LONG}.
	 * 
	 * @param buffer the data buffer.
	 * @return
	 */
	public static long asLong(ByteBuf buffer) {
		return buffer.readIntLE();
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
	 * @param buffer the data buffer.
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
