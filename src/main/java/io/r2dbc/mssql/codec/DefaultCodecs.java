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
import io.r2dbc.mssql.message.type.SqlServerType;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The default {@link Codec} implementation.  Delegates to type-specific codec implementations.
 */
public final class DefaultCodecs implements Codecs {

    private final List<Codec<?>> codecs;

    private final Map<SqlServerType, Codec<?>> codecPreferences = new HashMap<>();

    /**
     * Creates a new instance of {@link DefaultCodecs}.
     */
    public DefaultCodecs() {

        this.codecs = Arrays.asList(

            // Prioritized Codecs
            StringCodec.INSTANCE,

            BooleanCodec.INSTANCE,
            ByteCodec.INSTANCE,
            ShortCodec.INSTANCE,
            FloatCodec.INSTANCE,
            DoubleCodec.INSTANCE,
            IntegerCodec.INSTANCE,
            LongCodec.INSTANCE,
            LocalTimeCodec.INSTANCE,
            LocalDateCodec.INSTANCE,
            LocalDateTimeCodec.INSTANCE,
            UuidCodec.INSTANCE,
            DecimalCodec.INSTANCE,
            MoneyCodec.INSTANCE,
            TimestampCodec.INSTANCE,
            ZonedDateTimeCodec.INSTANCE
        );

        this.codecPreferences.put(SqlServerType.BIT, BooleanCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.TINYINT, ByteCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.SMALLINT, ShortCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.INTEGER, IntegerCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.BIGINT, LongCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.REAL, FloatCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.FLOAT, DoubleCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.GUID, UuidCodec.INSTANCE);
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

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends T> type) {

        Objects.requireNonNull(decodable, "Decodable must not be null");
        Objects.requireNonNull(type, "Type must not be null");

        if (buffer == null) {
            return null;
        }

        Codec<?> preferredCodec = this.codecPreferences.get(decodable.getType().getServerType());
        if (preferredCodec != null && preferredCodec.canDecode(decodable, type)) {
            return doDecode((Codec<T>) preferredCodec, buffer, decodable, type);
        }

        for (Codec<?> codec : this.codecs) {
            if (codec.canDecode(decodable, type)) {
                return doDecode((Codec<T>) codec, buffer, decodable, type);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot decode value of type [%s], name [%s] server type [%s]", type.getName(), decodable.getName(), decodable.getType().getServerType()));
    }

    @Nullable
    private <T> T doDecode(Codec<T> codec, @Nullable ByteBuf buffer, Decodable decodable, Class<? extends T> type) {
        return codec.decode(buffer, decodable, type);
    }
}
