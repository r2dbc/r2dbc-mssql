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
import io.netty.buffer.ByteBufUtil;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.message.type.TypeInformation.SqlServerType;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 * Codec for character values that are represented as {@link String}.
 *
 * <ul>
 * <li>Server types: (N)(VAR)CHAR, {@link SqlServerType#GUID}</li>
 * <li>Java type: {@link String}</li>
 * <li>Downcast: to {@link UUID#toString()}</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class StringCodec extends AbstractCodec<String> {

    /**
     * Singleton instance.
     */
    public static final StringCodec INSTANCE = new StringCodec();

    private static final Set<SqlServerType> SUPPORTED_TYPES = EnumSet.of(SqlServerType.CHAR, SqlServerType.NCHAR, SqlServerType.NVARCHAR, SqlServerType.VARCHAR, SqlServerType.GUID);

    private StringCodec() {
        super(String.class);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return SUPPORTED_TYPES.contains(typeInformation.getServerType());
    }

    @Override
    String doDecode(ByteBuf buffer, LengthDescriptor length, TypeInformation typeInformation, Class<? extends String> valueType) {

        if (length.isNull()) {
            return null;
        }

        if (typeInformation.getServerType() == SqlServerType.GUID) {

            UUID uuid = UuidCodec.INSTANCE.doDecode(buffer, length, typeInformation, UUID.class);
            return uuid != null ? uuid.toString().toUpperCase(Locale.ENGLISH) : null;
        }

        Charset charset = typeInformation.getCharset();

        String value = buffer.toString(buffer.readerIndex(), length.getLength(), charset);
        buffer.skipBytes(length.getLength());

        return valueType.cast(value);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, TypeInformation type, String value) {

        Charset charset = type.getCharset();

        return new Encoded(ByteBufUtil.encodeString(allocator, CharBuffer.wrap(value), charset), false);
    }
}
