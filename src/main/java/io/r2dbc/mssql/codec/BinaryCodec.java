/*
 * Copyright 2019-2022 the original author or authors.
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
import io.r2dbc.mssql.message.type.*;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Blob;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Codec for binary values that are represented as {@code byte[]} or {@link ByteBuffer}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#BINARY},  {@link SqlServerType#VARBINARY}, {@link SqlServerType#VARBINARYMAX}, and {@link SqlServerType#IMAGE}.</li>
 * <li>Java type: {@code byte[]}, {@link ByteBuffer}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
class BinaryCodec implements Codec<Object> {

    /**
     * Singleton instance.
     */
    public static final BinaryCodec INSTANCE = new BinaryCodec();

    private static final byte[] NULL = ByteArray.fromBuffer((alloc) ->
    {
        ByteBuf buffer = alloc.buffer(4);
        Encode.uShort(buffer, SqlServerType.VARBINARY.getMaxLength());
        Encode.uShort(buffer, Length.USHORT_NULL);

        return buffer;
    });

    private static final Set<SqlServerType> SUPPORTED_TYPES = EnumSet.of(SqlServerType.BINARY, SqlServerType.VARBINARY,
            SqlServerType.VARBINARYMAX, SqlServerType.IMAGE);

    private BinaryCodec() {
    }

    @Override
    public boolean canEncode(Object value) {

        Assert.requireNonNull(value, "Value must not be null");

        return value instanceof byte[] || value instanceof ByteBuffer;
    }

    @Override
    public Encoded encode(ByteBufAllocator allocator, RpcParameterContext context, Object value) {

        Assert.requireNonNull(allocator, "ByteBufAllocator must not be null");
        Assert.requireNonNull(context, "RpcParameterContext must not be null");
        Assert.requireNonNull(value, "Value must not be null");

        int length;
        if (value instanceof byte[]) {

            byte[] bytes = (byte[]) value;

            if (exceedsBigVarbinary(bytes.length)) {
                return BlobCodec.INSTANCE.encode(allocator, context, Blob.from(Mono.just(ByteBuffer.wrap(bytes))));
            }
            length = bytes.length;
        } else {

            ByteBuffer bytes = (ByteBuffer) value;

            if (exceedsBigVarbinary(bytes.remaining())) {
                return BlobCodec.INSTANCE.encode(allocator, context, Blob.from(Mono.just(bytes)));
            }

            length = bytes.remaining();
        }


        IntFunction<ByteBuf> encoder = actualLength -> {
            ByteBuf buffer;

            if (value instanceof byte[]) {

                byte[] bytes = (byte[]) value;

                buffer = RpcEncoding.prepareBuffer(allocator, TdsDataType.BIGVARBINARY.getLengthStrategy(), SqlServerType.VARBINARY.getMaxLength(), actualLength);
                buffer.writeBytes(bytes);
            } else {

                ByteBuffer bytes = (ByteBuffer) value;

                buffer = RpcEncoding.prepareBuffer(allocator, TdsDataType.BIGVARBINARY.getLengthStrategy(), SqlServerType.VARBINARY.getMaxLength(), actualLength);
                buffer.writeBytes(bytes.asReadOnlyBuffer());
            }
            return buffer;
        };

        return new VarbinaryEncoded(TdsDataType.BIGVARBINARY, Encoded.ofLengthAware(length, encoder));
    }

    @Override
    public boolean canEncodeNull(SqlServerType serverType) {
        return SUPPORTED_TYPES.contains(serverType);
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {

        Assert.requireNonNull(type, "Type must not be null");

        // Accept subtypes of ByteBuffer
        return type.isAssignableFrom(byte[].class) || ByteBuffer.class.isAssignableFrom(type);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Object> getType() {
        return (Class) ByteBuffer.class;
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator) {
        return new VarbinaryEncoded(TdsDataType.BIGVARBINARY, () -> Unpooled.wrappedBuffer(NULL));
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator, SqlServerType serverType) {
        return new VarbinaryEncoded(TdsDataType.BIGVARBINARY, () -> Unpooled.wrappedBuffer(NULL));
    }

    @Override
    public boolean canDecode(Decodable decodable, Class<?> type) {

        Assert.requireNonNull(decodable, "Decodable must not be null");
        Assert.requireNonNull(type, "Type must not be null");

        return SUPPORTED_TYPES.contains(decodable.getType().getServerType()) && canEncodeNull(type);
    }

    @Nullable
    public Object decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends Object> type) {

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

        if (length.isNull()) {
            return null;
        }

        return doDecode(buffer, length, decodable.getType(), type);
    }

    Object doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends Object> valueType) {

        byte[] bytes = new byte[length.getLength()];

        if (type.getLengthStrategy() == LengthStrategy.PARTLENTYPE) {

            int index = 0;
            while (buffer.isReadable()) {

                Length chunkLength = Length.decode(buffer, type);
                buffer.readBytes(bytes, index, chunkLength.getLength());
                index += chunkLength.getLength();
            }
        } else {
            buffer.readBytes(bytes);
        }

        // accept Object.class and ByteBuffer subclasses
        if (valueType.isAssignableFrom(ByteBuffer.class) || ByteBuffer.class.isAssignableFrom(valueType)) {
            return ByteBuffer.wrap(bytes);
        }

        return bytes;
    }

    static class VarbinaryEncoded extends RpcEncoding.HintedEncoded {

        private static final String FORMAL_TYPE = SqlServerType.VARBINARY + "(" + TypeUtils.SHORT_VARTYPE_MAX_BYTES + ")";

        VarbinaryEncoded(TdsDataType dataType, Supplier<ByteBuf> value) {
            super(dataType, SqlServerType.VARBINARY, value);
        }

        @Override
        public String getFormalType() {
            return FORMAL_TYPE;
        }

    }

    private static boolean exceedsBigVarbinary(int length) {
        return length > TypeUtils.SHORT_VARTYPE_MAX_BYTES;
    }

}
