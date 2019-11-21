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
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;
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

    private static final byte[] NULL = ByteArray.fromBuffer(alloc -> {

        ByteBuf buffer = alloc.buffer(8);

        Encode.uShort(buffer, TypeUtils.SHORT_VARTYPE_MAX_BYTES);
        Collation.RAW.encode(buffer);
        Encode.uShort(buffer, -1);

        return buffer;
    });

    private StringCodec() {
        super(String.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, String value) {

        RpcParameterContext.CharacterValueContext valueContext = context.getRequiredValueContext(RpcParameterContext.CharacterValueContext.class);
        TdsDataType dataType = getDataType(context.getDirection(), valueContext.isSendStringParametersAsUnicode(), value);
        ByteBuf buffer = allocator.buffer((value.length() * 2) + 7);

        doEncode(buffer, dataType, context.getDirection(), valueContext.getCollation(), value);

        if (dataType == TdsDataType.NVARCHAR || dataType == TdsDataType.NCHAR) {
            return new NvarcharEncoded(dataType, buffer);
        }

        return new VarcharEncoded(dataType, buffer);
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return new VarcharEncoded(TdsDataType.NVARCHAR, Unpooled.wrappedBuffer(NULL));
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

            StringBuilder result = new StringBuilder();

            while (buffer.isReadable()) {

                Length chunkLength = Length.decode(buffer, typeInformation);

                result.append(buffer.toString(buffer.readerIndex(), chunkLength.getLength(), charset));
                buffer.skipBytes(chunkLength.getLength());
            }

            return result.toString();
        }

        String value = buffer.toString(buffer.readerIndex(), length.getLength(), charset);
        buffer.skipBytes(length.getLength());

        return valueType.cast(value);
    }

    static TdsDataType getDataType(RpcDirection direction, boolean sendStringParametersAsUnicode, @Nullable String value) {

        int valueLength = value == null ? 0 : (value.length() * 2);
        boolean isShortValue = valueLength <= TypeUtils.SHORT_VARTYPE_MAX_BYTES;

        // Use PLP encoding on Yukon and later with long values and OUT parameters
        boolean usePLP = (!isShortValue || direction == RpcDirection.OUT);

        if (sendStringParametersAsUnicode) {
            return usePLP ? TdsDataType.NTEXT : TdsDataType.NVARCHAR;
        }

        return usePLP ? TdsDataType.TEXT : TdsDataType.BIGVARCHAR;
    }

    static void doEncode(ByteBuf buffer, TdsDataType dataType, RpcDirection direction, Collation collation, @Nullable String value) {

        boolean isNull = (value == null);

        ByteBuf characterData = isNull ? Unpooled.EMPTY_BUFFER : doEncode(buffer.alloc(), dataType, collation, value);
        int valueLength = characterData.readableBytes();
        boolean isShortValue = valueLength <= TypeUtils.SHORT_VARTYPE_MAX_BYTES;

        // Textual RPC requires a collation. If none is provided, as is the case when
        // the SSType is non-textual, then use the database collation by default.

        // Use PLP encoding on Yukon and later with long values and OUT parameters
        boolean usePLP = (!isShortValue || direction == RpcDirection.OUT);
        if (usePLP) {

            // Handle Yukon v*max type header here.
            writeVMaxHeader(valueLength,    // Length
                isNull,    // Is null?
                collation, buffer);

            // Send the data.
            if (!isNull) {
                if (valueLength > 0) {

                    Encode.asInt(buffer, valueLength);
                    buffer.writeBytes(characterData);
                    characterData.release();
                }

                // Send the terminator PLP chunk.
                Encode.asInt(buffer, 0);
            }
        } else // non-PLP type
        {
            // Write maximum length of data
            Encode.uShort(buffer, TypeUtils.SHORT_VARTYPE_MAX_BYTES);

            collation.encode(buffer);

            // Data and length
            if (isNull) {
                Encode.uShort(buffer, -1); // actual len
            } else {
                // Write actual length of data
                Encode.uShort(buffer, valueLength);

                // If length is zero, we're done.
                if (0 != valueLength) {
                    buffer.writeBytes(characterData);
                    characterData.release();
                }
            }
        }
    }

    private static ByteBuf doEncode(ByteBufAllocator alloc, TdsDataType dataType, Collation collation, String value) {

        if (value.isEmpty()) {
            return Unpooled.EMPTY_BUFFER;
        }

        if (dataType == TdsDataType.BIGVARCHAR || dataType == TdsDataType.TEXT) {
            ByteBuf buffer = alloc.buffer(value.length());
            Encode.rpcString(buffer, value, collation.getCharset());
            return buffer;
        }

        ByteBuf buffer = alloc.buffer(value.length() * 2);
        Encode.rpcString(buffer, value);
        return buffer;
    }

    /**
     * Appends a standard v*max header for RPC parameter transmission.
     *
     * @param headerLength the total length of the PLP data block.
     * @param isNull       true if the value is NULL.
     * @param collation    The SQL collation associated with the value that follows the v*max header. Null for non-textual types.
     */
    private static void writeVMaxHeader(long headerLength,
                                        boolean isNull,
                                        @Nullable Collation collation, ByteBuf buffer) {
        // Send v*max length indicator 0xFFFF.
        Encode.uShort(buffer, 0xFFFF);

        if (collation != null) {
            collation.encode(buffer);
        }

        // Handle null here and return, we're done here if it's null.
        if (isNull) {
            // Null header for v*max types is 0xFFFFFFFFFFFFFFFF.
            Encode.uLongLong(buffer, 0xFFFFFFFFFFFFFFFFL);
        } else if (TypeUtils.UNKNOWN_STREAM_LENGTH == headerLength) {
            // Append v*max length.
            // UNKNOWN_PLP_LEN is 0xFFFFFFFFFFFFFFFE
            Encode.uLongLong(buffer, 0xFFFFFFFFFFFFFFFEL);

            // NOTE: Don't send the first chunk length, this will be calculated by caller.
        } else {
            // For v*max types with known length, length is <totallength8><chunklength4>
            // We're sending same total length as chunk length (as we're sending 1 chunk).
            Encode.uLongLong(buffer, headerLength);
        }
    }

    static class NvarcharEncoded extends RpcEncoding.HintedEncoded {

        private static final String FORMAL_TYPE = SqlServerType.NVARCHAR + "(" + (TypeUtils.SHORT_VARTYPE_MAX_BYTES / 2) + ")";

        NvarcharEncoded(TdsDataType dataType, ByteBuf value) {
            super(dataType, SqlServerType.NVARCHAR, value);
        }

        @Override
        public String getFormalType() {
            return FORMAL_TYPE;
        }
    }

    static class VarcharEncoded extends RpcEncoding.HintedEncoded {

        private static final String FORMAL_TYPE = SqlServerType.VARCHAR + "(" + TypeUtils.SHORT_VARTYPE_MAX_BYTES + ")";

        VarcharEncoded(TdsDataType dataType, ByteBuf value) {
            super(dataType, SqlServerType.NVARCHAR, value);
        }

        @Override
        public String getFormalType() {
            return FORMAL_TYPE;
        }
    }
}
