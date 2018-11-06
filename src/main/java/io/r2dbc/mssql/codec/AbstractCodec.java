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
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.TypeInformation;
import reactor.util.annotation.Nullable;

import java.util.Objects;

/**
 * Abstract codec class that provides a basis for all concrete
 * implementations of a  {@link Codec}.
 *
 * @param <T> the type that is handled by this {@link Codec}.
 */
abstract class AbstractCodec<T> implements Codec<T> {

    private final Class<T> type;

    /**
     * Creates a new {@link AbstractCodec}.
     *
     * @param type the type handled by this codec.
     */
    AbstractCodec(Class<T> type) {
        this.type = Objects.requireNonNull(type, "Type must not be null");
    }

    @Override
    public boolean canDecode(Decodable decodable, Class<?> type) {

        Objects.requireNonNull(decodable, "Decodable must not be null");
        Objects.requireNonNull(type, "Type must not be null");

        return type.isAssignableFrom(this.type) &&
            doCanDecode(decodable.getType());
    }

    @Nullable
    public final T decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends T> type) {

        if (buffer == null) {
            return null;
        }

        Length length = Length.decode(buffer, decodable.getType());
        return doDecode(buffer, length, decodable.getType(), type);
    }

    @Override
    public ByteBuf encode(ByteBufAllocator allocator, TypeInformation typeInformation, T value) {

        Objects.requireNonNull(allocator, "ByteBufAllocator must not be null");
        Objects.requireNonNull(typeInformation, "TypeInformation must not be null");
        Objects.requireNonNull(value, "Value must not be null");


        Encoded encoded = doEncode(allocator, typeInformation, value);
        Length length = Length.of(encoded.encoded.readableBytes(), encoded.isNull);

        ByteBuf buffer = allocator.buffer(encoded.encoded.readableBytes() + 2);

        length.encode(buffer, typeInformation);
        buffer.writeBytes(encoded.encoded);
        encoded.encoded.release();

        return buffer;
    }

    /**
     * Determine whether this {@link Codec} is capable of decoding column values based on the given {@link TypeInformation}.
     *
     * @param typeInformation the column type.
     * @return {@literal true} if this codec is able to decode values of {@link TypeInformation}.
     */
    abstract boolean doCanDecode(TypeInformation typeInformation);

    /**
     * Decode the {@link ByteBuf data} into the {@link Class value type}.
     *
     * @param buffer    the data buffer.
     * @param length    length of the column data.
     * @param type      the type descriptor.
     * @param valueType the desired value type.
     * @return the decoded value. Can be {@literal null} if the column value is {@literal null}.
     */
    @Nullable
    abstract T doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends T> valueType);

    abstract Encoded doEncode(ByteBufAllocator allocator, TypeInformation type, T value);

    /**
     * Wrapper for encoded values.
     */
    static class Encoded {

        ByteBuf encoded;

        boolean isNull;

        private Encoded(ByteBuf encoded) {
            this(encoded, false);
        }

        Encoded(ByteBuf encoded, boolean isNull) {
            this.encoded = encoded;
            this.isNull = isNull;
        }

        static Encoded of(ByteBuf encoded) {
            return new Encoded(encoded);
        }
    }
}
