/*
 * Copyright 2019-2020 the original author or authors.
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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.codec.RpcParameterContext.CharacterValueContext;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;
import io.r2dbc.mssql.message.type.TypeUtils;
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.CharBuffer;

/**
 * Basic {@link CharSequence} encoding utilities.
 *
 * @author Mark Paluch
 */
class CharacterEncoder {

    private static final byte[] NULL = ByteArray.fromBuffer(alloc -> {

        ByteBuf buffer = alloc.buffer(8);

        Encode.uShort(buffer, TypeUtils.SHORT_VARTYPE_MAX_BYTES);
        Collation.RAW.encode(buffer);
        Encode.uShort(buffer, -1);

        return buffer;
    });

    /**
     * Encode a {@code VARCHAR NULL}.
     *
     * @return the {@link Encoded} {@code VARCHAR NULL}.
     */
    static Encoded encodeNull() {
        return new VarcharEncoded(TdsDataType.NVARCHAR, Unpooled.wrappedBuffer(NULL));
    }

    /**
     * Encode a {@link CharSequence} to {@code VARCHAR} or {@code NVARCHAR} depending on {@code sendStringParametersAsUnicode}.
     *
     * @return the {@link Encoded} {@link CharSequence}.
     */
    static Encoded encodeBigVarchar(ByteBufAllocator allocator, RpcDirection direction, Collation collation, boolean sendStringParametersAsUnicode, @Nullable CharSequence value) {

        ByteBuf buffer = allocator.buffer((value != null ? value.length() * 2 : 0) + 7);

        if (sendStringParametersAsUnicode) {

            encodeBigVarchar(buffer, direction, collation, true, value);
            return new NvarcharEncoded(TdsDataType.NVARCHAR, buffer);
        }

        encodeBigVarchar(buffer, direction, collation, false, value);
        return new VarcharEncoded(TdsDataType.BIGVARCHAR, buffer);
    }

    /**
     * Encode a {@link CharSequence} to {@code VARCHAR} or {@code NVARCHAR} depending on {@code sendStringParametersAsUnicode}. Uses either {@code (N)VARCHAR} or {@code (N)VARCHAR(MAX)}, depending
     * on the string size.
     */
    static void encodeBigVarchar(ByteBuf buffer, RpcDirection direction, Collation collation, boolean sendStringParametersAsUnicode, @Nullable CharSequence value) {

        ByteBuf characterData = encodeCharSequence(buffer.alloc(), collation, sendStringParametersAsUnicode, value);
        int valueLength = characterData.readableBytes();
        boolean isShortValue = valueLength <= TypeUtils.SHORT_VARTYPE_MAX_BYTES;
        boolean isNull = value == null;

        // Textual RPC requires a collation. If none is provided, as is the case when
        // the SSType is non-textual, then use the database collation by default.

        // Use PLP encoding on Yukon and later with long values and OUT parameters
        boolean usePLP = (!isShortValue || direction == RpcDirection.OUT);
        if (usePLP) {

            // Send v*max length indicator 0xFFFF.
            Encode.uShort(buffer, (short) 0xFFFF);

            // Send collation if requested.
            collation.encode(buffer);

            // Handle null here and return, we're done here if it's null.
            if (isNull) {
                // Null header for v*max types is 0xFFFFFFFFFFFFFFFF.
                Encode.uLongLong(buffer, 0xFFFFFFFFFFFFFFFFL);
            } else if (Length.UNKNOWN_STREAM_LENGTH == valueLength) {
                // Append v*max length.
                // UNKNOWN_PLP_LEN is 0xFFFFFFFFFFFFFFFE
                Encode.uLongLong(buffer, 0xFFFFFFFFFFFFFFFEL);

                // NOTE: Don't send the first chunk length, this will be calculated by caller.
            } else {
                // For v*max types with known length, length is <totallength8><chunklength4>
                // We're sending same total length as chunk length (as we're sending 1 chunk).
                Encode.uLongLong(buffer, valueLength);
            }

            // Send the data.
            if (!isNull) {
                if (valueLength > 0) {
                    Encode.asInt(buffer, valueLength);
                    buffer.writeBytes(characterData);
                    characterData.release();
                }
            }

            // Send the terminator PLP chunk.
            Encode.asInt(buffer, 0);

        } else {

            // Write maximum length of data
            Encode.uShort(buffer, TypeUtils.SHORT_VARTYPE_MAX_BYTES);

            collation.encode(buffer);

            // Write actual length of data
            Encode.uShort(buffer, valueLength);

            // If length is zero, we're done.
            if (0 != valueLength) {
                buffer.writeBytes(characterData);
                characterData.release();
            }
        }
    }

    private static ByteBuf encodeCharSequence(ByteBufAllocator alloc, Collation collation, boolean sendStringParametersAsUnicode, @Nullable CharSequence value) {

        if (value == null || value.length() == 0) {
            return Unpooled.EMPTY_BUFFER;
        }

        if (sendStringParametersAsUnicode) {
            ByteBuf buffer = alloc.buffer(value.length() * 2);
            Encode.rpcString(buffer, value);
            return buffer;
        }

        ByteBuf buffer = alloc.buffer((int) (value.length() * 1.5));
        Encode.rpcString(buffer, value, collation.getCharset());
        return buffer;
    }

    static Encoded encodePlp(ByteBufAllocator allocator, CharacterValueContext valueContext, CharSequence value) {

        Flux<ByteBuf> binaryStream = Flux.just(value).map(it -> {
            return encodeCharSequence(allocator, valueContext, it);
        });

        return new PlpEncodedCharacters(getPlpType(valueContext), valueContext.getCollation(), allocator, binaryStream, () -> {
        });
    }

    static Encoded encodePlp(ByteBufAllocator allocator, CharacterValueContext valueContext, Clob value) {

        Flux<ByteBuf> binaryStream = Flux.from(value.stream()).map(it -> {
            return encodeCharSequence(allocator, valueContext, it);
        });

        return new PlpEncodedCharacters(getPlpType(valueContext), valueContext.getCollation(), allocator, binaryStream, () -> Mono.from(value.discard()).toFuture());
    }

    private static SqlServerType getPlpType(CharacterValueContext valueContext) {
        return valueContext.isSendStringParametersAsUnicode() ? SqlServerType.NVARCHARMAX : SqlServerType.VARCHARMAX;
    }

    private static ByteBuf encodeCharSequence(ByteBufAllocator allocator, CharacterValueContext valueContext, CharSequence it) {
        return ByteBufUtil.encodeString(allocator, CharBuffer.wrap(it), valueContext.isSendStringParametersAsUnicode() ? ServerCharset.UNICODE.charset() : valueContext.getCollation().getCharset());
    }

    private static class NvarcharEncoded extends RpcEncoding.HintedEncoded {

        private static final String FORMAL_TYPE = SqlServerType.NVARCHAR + "(" + (TypeUtils.SHORT_VARTYPE_MAX_BYTES / 2) + ")";

        NvarcharEncoded(TdsDataType dataType, ByteBuf value) {
            super(dataType, SqlServerType.NVARCHAR, value);
        }

        @Override
        public String getFormalType() {
            return FORMAL_TYPE;
        }

    }

    private static class VarcharEncoded extends RpcEncoding.HintedEncoded {

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
