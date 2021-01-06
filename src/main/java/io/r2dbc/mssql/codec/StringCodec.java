/*
 * Copyright 2018-2020 the original author or authors.
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
import io.netty.buffer.CompositeByteBuf;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.message.type.TypeUtils;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 * Codec for character values that are represented as {@link String}.
 *
 * <ul>
 * <li>Server types: (N)(VAR)CHAR, (N)TEXT {@link SqlServerType#GUID}</li>
 * <li>Java type: {@link String}</li>
 * <li>Downcast: to {@link UUID#toString()}</li>
 * </ul>
 *
 * @author Mark Paluch
 * @author Anton Duyun
 */
final class StringCodec extends AbstractCodec<String> {

    /**
     * Singleton instance.
     */
    static final StringCodec INSTANCE = new StringCodec();

    private static final Set<SqlServerType> SUPPORTED_TYPES = EnumSet.of(SqlServerType.CHAR, SqlServerType.NCHAR,
        SqlServerType.VARCHAR, SqlServerType.NVARCHAR,
        SqlServerType.VARCHARMAX, SqlServerType.NVARCHARMAX,
        SqlServerType.TEXT, SqlServerType.NTEXT,
        SqlServerType.GUID);

    private StringCodec() {
        super(String.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, String value) {

        RpcParameterContext.CharacterValueContext valueContext = context.getRequiredValueContext(RpcParameterContext.CharacterValueContext.class);

        SqlServerType serverType = context.getServerType();

        if (exceedsBigVarchar(context.getDirection(), value) || serverType == SqlServerType.VARCHARMAX || serverType == SqlServerType.NVARCHARMAX) {
            return CharacterEncoder.encodePlp(allocator, serverType, valueContext, value);
        }

        return CharacterEncoder.encodeBigVarchar(allocator, context.getDirection(), serverType, valueContext.getCollation(), valueContext.isSendStringParametersAsUnicode(), value);
    }

    @Override
    public boolean canEncodeNull(SqlServerType serverType) {
        return serverType == SqlServerType.VARCHAR || serverType == SqlServerType.NVARCHAR;
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return encodeNull(allocator, SqlServerType.NVARCHAR);
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator, SqlServerType serverType) {
        return CharacterEncoder.encodeNull(serverType);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return SUPPORTED_TYPES.contains(typeInformation.getServerType());
    }

    @Nullable
    public String decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends String> type) {

        Assert.requireNonNull(decodable, "Decodable must not be null");
        Assert.requireNonNull(type, "Type must not be null");

        if (buffer == null) {
            return null;
        }

        Length length;

        if (decodable.getType().getLengthStrategy() == LengthStrategy.PARTLENTYPE) {
            PlpLength plpLength = PlpLength.decode(buffer, decodable.getType());
            length = Length.of(Math.toIntExact(plpLength.getLength()), plpLength.isNull());
        } else {
            length = Length.decode(buffer, decodable.getType());
        }

        return doDecode(buffer, length, decodable.getType(), type);
    }

    @Override
    String doDecode(ByteBuf buffer, Length length, TypeInformation typeInformation, Class<? extends String> valueType) {

        if (length.isNull()) {
            return null;
        }

        if (typeInformation.getServerType() == SqlServerType.GUID) {

            UUID uuid = UuidCodec.INSTANCE.doDecode(buffer, length, typeInformation, UUID.class);
            return uuid != null ? uuid.toString().toUpperCase(Locale.ENGLISH) : null;
        }

        Charset charset = typeInformation.getCharset();

        if (typeInformation.getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            CompositeByteBuf result = buffer.alloc().compositeBuffer();

            try {
                while (buffer.isReadable()) {

                    Length chunkLength = Length.decode(buffer, typeInformation);
                    result.addComponent(true, buffer.readRetainedSlice(chunkLength.getLength()));
                }

                return result.toString(charset);
            } finally {
                result.release();
            }
        }

        String value = buffer.toString(buffer.readerIndex(), length.getLength(), charset);
        buffer.skipBytes(length.getLength());

        return valueType.cast(value);
    }

    static boolean exceedsBigVarchar(RpcDirection direction, String value) {

        int valueLength = (value.length() * 2);
        boolean isShortValue = valueLength <= TypeUtils.SHORT_VARTYPE_MAX_BYTES;

        // Use PLP encoding on Yukon and later with long values and OUT parameters
        return (!isShortValue || direction == RpcDirection.OUT);
    }

}
