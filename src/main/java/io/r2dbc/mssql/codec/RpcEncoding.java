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
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.mssql.util.StringUtils;
import reactor.util.annotation.Nullable;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Utility methods to encode RPC parameters.
 *
 * @author Mark Paluch
 */
public final class RpcEncoding {

    private RpcEncoding() {
    }

    /**
     * Encode a string parameter as {@literal NVARCHAR} or {@literal NTEXT} (depending on the size).
     *
     * @param buffer    the data buffer.
     * @param name      optional parameter name.
     * @param direction RPC parameter direction (in/out)
     * @param collation parameter value encoding.
     * @param value     the parameter value, can be {@code null}.
     */
    public static void encodeString(ByteBuf buffer, @Nullable String name, RpcDirection direction, Collation collation, @Nullable String value) {

        encodeHeader(buffer, name, direction, TdsDataType.NVARCHAR);
        CharacterEncoder.encodeBigVarchar(buffer, direction, collation, true, value);
    }

    /**
     * Encode an integer parameter as {@literal INTn}.
     *
     * @param buffer    the data buffer.
     * @param name      optional parameter name.
     * @param direction RPC parameter direction (in/out)
     * @param value     the parameter value, can be {@code null}.
     */
    public static void encodeInteger(ByteBuf buffer, @Nullable String name, RpcDirection direction, @Nullable Integer value) {

        encodeHeader(buffer, name, direction, TdsDataType.INTN);

        Encode.asByte(buffer, 4); // max-len

        if (value == null) {
            Encode.asByte(buffer, 0); // len of data bytes
        } else {
            Encode.asByte(buffer, 4); // len of data bytes
            Encode.asInt(buffer, value);
        }
    }

    /**
     * Encode an RPC header that writes {@code name}, {@link RpcDirection}, and the {@link TdsDataType}.
     *
     * @param buffer    the data buffer.
     * @param name      name of the parameter, can be {@code null}.
     * @param direction the parameter direction.
     * @param dataType  TDS data type.
     */
    public static void encodeHeader(ByteBuf buffer, @Nullable String name, RpcDirection direction, TdsDataType dataType) {

        if (StringUtils.hasText(name)) {

            Encode.asByte(buffer, name.length() + 1);

            char at = '@';
            writeChar(buffer, at);

            for (int i = 0; i < name.length(); i++) {
                char ch = name.charAt(i);
                writeChar(buffer, ch);
            }
        } else {
            Encode.asByte(buffer, 0);
        }

        Encode.asByte(buffer, direction == RpcDirection.OUT ? 1 : 0);
        Encode.asByte(buffer, dataType.getValue());
    }

    private static void writeChar(ByteBuf buffer, char ch) {

        buffer.writeByte((byte) (ch & 0xFF));
        buffer.writeByte((byte) ((ch >> 8) & 0xFF));
    }

    /**
     * Encode a RPC parameter that uses a fixed-length, nullable data type.
     *
     * @param allocator    the allocator to allocate encoding buffers.
     * @param serverType   the server type. Used to derive the nullable {@link TdsDataType}.
     * @param value        the value to encode.
     * @param valueEncoder encoder function. Using a {@link BiFunction} to allow non-capturing lambdas.
     * @param <T>
     * @return
     */
    public static <T> Encoded encodeFixed(ByteBufAllocator allocator, SqlServerType serverType, T value, BiConsumer<ByteBuf, T> valueEncoder) {

        Assert.notNull(serverType.getNullableType(), "Server type provides no nullable type");
        LengthStrategy lengthStrategy = serverType.getNullableType().getLengthStrategy();
        ByteBuf buffer = prepareBuffer(allocator, lengthStrategy, serverType.getMaxLength(), serverType.getMaxLength());

        valueEncoder.accept(buffer, value);

        return new HintedEncoded(serverType.getNullableType(), serverType, buffer);
    }

    /**
     * Encode a RPC parameter that declares length and max-length attributes and apply a {@link SqlServerType} hint.
     *
     * @param allocator    the allocator to allocate encoding buffers.
     * @param serverType   the server data type. Used to derive the nullable {@link TdsDataType}.
     * @param length       actual data length.
     * @param value        the value to encode.
     * @param valueEncoder encoder function. Using a {@link BiFunction} to allow non-capturing lambdas.
     * @param <T>
     * @return
     */
    public static <T> Encoded encode(ByteBufAllocator allocator, SqlServerType serverType, int length, T value, BiConsumer<ByteBuf, T> valueEncoder) {

        Assert.notNull(serverType.getNullableType(), "Server type provides no nullable type");

        TdsDataType dataType = serverType.getNullableType();
        ByteBuf buffer = prepareBuffer(allocator, dataType.getLengthStrategy(), serverType.getMaxLength(), length);

        valueEncoder.accept(buffer, value);

        return new HintedEncoded(dataType, serverType, buffer);
    }

    /**
     * Encode a {@code null} RPC parameter that declares length and max-length attributes and apply a {@link SqlServerType} hint.
     *
     * @param allocator  the allocator to allocate encoding buffers.
     * @param serverType the server data type. Used to derive the nullable {@link TdsDataType}.
     * @return the encoded {@code null} value.
     */
    public static Encoded encodeNull(ByteBufAllocator allocator, SqlServerType serverType) {

        Assert.isTrue(serverType.getMaxLength() > 0, "Server type does not declare a max length");
        Assert.notNull(serverType.getNullableType(), "Server type does not declare a nullable type");
        ByteBuf buffer = prepareBuffer(allocator, serverType.getNullableType().getLengthStrategy(), serverType.getMaxLength(), 0);

        return new HintedEncoded(serverType.getNullableType(), serverType, buffer);
    }

    /**
     * Wrap a binary encoded RPC parameter and apply a {@link SqlServerType} hint.
     *
     * @param buffer     the encoded buffer.
     * @param serverType the server data type. Used to derive the nullable {@link TdsDataType}.
     * @return the encoded {@code null} value.
     */
    public static Encoded wrap(byte[] buffer, SqlServerType serverType) {

        Assert.isTrue(serverType.getMaxLength() > 0, "Server type does not declare a max length");
        Assert.notNull(serverType.getNullableType(), "Server type does not declare a nullable type");

        return new HintedEncoded(serverType.getNullableType(), serverType, Unpooled.wrappedBuffer(buffer));
    }

    /**
     * Encode a temporal typed {@code null} RPC parameter.
     *
     * @param allocator  the allocator to allocate encoding buffers.
     * @param serverType the server data type. Used to derive the nullable {@link TdsDataType}.
     * @return the encoded {@code null} value.
     */
    public static Encoded encodeTemporalNull(ByteBufAllocator allocator, SqlServerType serverType) {

        Assert.notNull(serverType.getNullableType(), "Server type does not declare a nullable type");
        ByteBuf buffer = allocator.buffer(1);

        Encode.asByte(buffer, 0);

        return new HintedEncoded(serverType.getNullableType(), serverType, buffer);
    }

    /**
     * Encode a temporal scaled {@code null} RPC parameter.
     *
     * @param allocator  the allocator to allocate encoding buffers.
     * @param serverType the server data type. Used to derive the nullable {@link TdsDataType}.
     * @param scale      type scale.
     * @return the encoded {@code null} value.
     */
    public static Encoded encodeTemporalNull(ByteBufAllocator allocator, SqlServerType serverType, int scale) {

        Assert.notNull(serverType.getNullableType(), "Server type does not declare a nullable type");
        ByteBuf buffer = allocator.buffer(1);

        Encode.asByte(buffer, scale);
        Encode.asByte(buffer, 0); // value length

        return new HintedEncoded(serverType.getNullableType(), serverType, buffer);
    }

    static ByteBuf prepareBuffer(ByteBufAllocator allocator, LengthStrategy lengthStrategy, int maxLength, int length) {

        ByteBuf buffer;
        switch (lengthStrategy) {
            case PARTLENTYPE:

                buffer = allocator.buffer(8 + 8 + length);
                buffer.writeLong(maxLength).writeLong(length);

                return buffer;

            case BYTELENTYPE:

                buffer = allocator.buffer(1 + 1 + length);
                Encode.asByte(buffer, maxLength);
                Encode.asByte(buffer, length);
                return buffer;

            case FIXEDLENTYPE:

                buffer = allocator.buffer();
                return buffer;

            case USHORTLENTYPE:

                buffer = allocator.buffer(1 + 1 + length);
                Encode.uShort(buffer, maxLength);
                Encode.uShort(buffer, length);


                return buffer;
            default:
                throw new UnsupportedOperationException(lengthStrategy.toString());
        }
    }

    /**
     * Extension to {@link Encoded} that applies a {@link SqlServerType} hint.
     */
    static class HintedEncoded extends Encoded {

        private final SqlServerType sqlServerType;

        public HintedEncoded(TdsDataType dataType, SqlServerType sqlServerType, ByteBuf value) {
            super(dataType, value);
            this.sqlServerType = sqlServerType;
        }

        @Override
        public String getFormalType() {
            return this.sqlServerType.toString();
        }
    }
}
