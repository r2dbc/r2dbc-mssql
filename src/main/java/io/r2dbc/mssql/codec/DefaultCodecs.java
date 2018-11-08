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
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The default {@link Codec} implementation.  Delegates to type-specific codec implementations.
 */
public final class DefaultCodecs implements Codecs {

    private final List<Codec<?>> codecs;

    /**
     * Creates a new instance of {@link DefaultCodecs}.
     */
    public DefaultCodecs() {

        this.codecs = Arrays.asList(

            // Prioritized Codecs
            StringCodec.INSTANCE,

            FloatCodec.INSTANCE,
            IntegerCodec.INSTANCE,
            LongCodec.INSTANCE,
            ShortCodec.INSTANCE,
            ByteCodec.INSTANCE,
            BooleanCodec.INSTANCE,
            LocalTimeCodec.INSTANCE,
            LocalDateCodec.INSTANCE,
            LocalDateTimeCodec.INSTANCE,
            UuidCodec.INSTANCE,
            DecimalCodec.INSTANCE,
            MoneyCodec.INSTANCE,
            TimestampCodec.INSTANCE,
            ZonedDateTimeCodec.INSTANCE
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends T> type) {

        Objects.requireNonNull(decodable, "Decodable must not be null");
        Objects.requireNonNull(type, "Type must not be null");

        if (buffer == null) {
            return null;
        }

        for (Codec<?> codec : this.codecs) {
            if (codec.canDecode(decodable, type)) {
                return ((Codec<T>) codec).decode(buffer, decodable, type);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot decode value of type [%s], name [%s] server type [%s]", type.getName(), decodable.getName(), decodable.getType().getServerType()));
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator, Class<?> type) {

        Objects.requireNonNull(allocator, "ByteBufAllocator must not be null");
        Objects.requireNonNull(type, "Type must not be null");

        for (Codec<?> codec : this.codecs) {
            if (codec.canEncodeNull(type)) {
                return codec.encodeNull(allocator);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot encode [null] parameter of type [%s]", type.getName()));
    }

    @SuppressWarnings({"unchecked", "rawtpes"})
    @Override
    public Encoded encode(ByteBufAllocator allocator, RpcParameterContext context, Object value) {

        Objects.requireNonNull(allocator, "ByteBufAllocator must not be null");
        Objects.requireNonNull(context, "RpcParameterContext must not be null");
        Objects.requireNonNull(value, "Value must not be null");

        for (Codec<?> codec : this.codecs) {
            if (codec.canEncode(value)) {
                return ((Codec) codec).encode(allocator, context, value);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot encode [%s] parameter of type [%s]", value, value.getClass().getName()));
    }
}
