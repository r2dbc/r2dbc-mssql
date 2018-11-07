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
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.TdsDataType;
import io.r2dbc.mssql.message.type.TypeInformation.LengthStrategy;
import io.r2dbc.mssql.util.Assert;
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
     *  @param buffer    the data buffer.
     * @param name      optional parameter name.
     * @param direction RPC parameter direction (in/out)
     * @param collation parameter value encoding.
     * @param value     the parameter value, can be {@literal null}.
     */
    public static void encodeString(ByteBuf buffer, @Nullable String name, RpcDirection direction, Collation collation, @Nullable String value) {

        TdsDataType dataType = StringCodec.getDataType(direction, value);

        encodeHeader(buffer, name, direction, dataType);
        StringCodec.doEncode(buffer, direction, collation, value);
    }

    /**
     * Encode an integer parameter as {@literal INTn}.
     *  @param buffer    the data buffer.
     * @param name      optional parameter name.
     * @param direction RPC parameter direction (in/out)
     * @param value     the parameter value, can be {@literal null}.
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
     * @param name      name of the parameter, can be {@literal null}.
     * @param direction the parameter direction.
     * @param dataType  TDS data type.
     */
    public static void encodeHeader(ByteBuf buffer, @Nullable String name, RpcDirection direction, TdsDataType dataType) {

        if (name != null) {

            Encode.asByte(buffer, (name.length() + 1) * 2);

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
     * Encode a RPC parameter that declares length and max-length attributes.
     *
     * @param allocator
     * @param dataType
     * @param maxLength
     * @param length
     * @param value        the value to encode.
     * @param valueEncoder encoder function. Using a {@link BiFunction} to allow non-capturing lambdas.
     * @param <T>
     * @return
     */
    public static <T> Encoded encode(ByteBufAllocator allocator, TdsDataType dataType, int maxLength, int length, T value, BiConsumer<ByteBuf, T> valueEncoder) {

        ByteBuf buffer = prepareBuffer(allocator, dataType.getLengthStrategy(), maxLength, length);

        valueEncoder.accept(buffer, value);

        return Encoded.of(dataType, buffer);
    }

    /**
     * Encode a RPC parameter with a fixed length.
     *
     * @param allocator
     * @param dataType     TDS data type.
     * @param value        the value to encode.
     * @param valueEncoder encoder function. Using a {@link BiFunction} to allow non-capturing lambdas.
     * @param <T>
     * @return
     * @throws IllegalArgumentException if the length strategy is not fixes.
     * @see LengthStrategy#FIXEDLENTYPE
     */
    public static <T> Encoded encode(ByteBufAllocator allocator, TdsDataType dataType, T value, BiConsumer<ByteBuf, T> valueEncoder) {

        Assert.isTrue(dataType.getLengthStrategy() == LengthStrategy.FIXEDLENTYPE, "Data type is not a FIXEDLENGTH type");

        ByteBuf buffer = prepareBuffer(allocator, dataType.getLengthStrategy(), 0, 0);

        valueEncoder.accept(buffer, value);

        return Encoded.of(dataType, buffer);
    }

    private static ByteBuf prepareBuffer(ByteBufAllocator allocator, LengthStrategy lengthStrategy, int maxLength, int length) {

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
}
