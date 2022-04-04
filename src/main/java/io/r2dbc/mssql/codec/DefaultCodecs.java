/*
 * Copyright 2018-2022 the original author or authors.
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
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Type;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The default {@link Codec} implementation.  Delegates to type-specific codec implementations.
 */
public final class DefaultCodecs implements Codecs {

    private final Codec<?>[] codecs;

    private final Map<SqlServerType, Codec<?>> codecPreferences = new HashMap<>();

    private final Map<Class<?>, Codec<?>> codecNullCache = new ConcurrentHashMap<>();

    /**
     * Creates a new instance of {@link DefaultCodecs}.
     */
    @SuppressWarnings("rawtypes")
    public DefaultCodecs() {

        this.codecs = Arrays.asList(

            // Prioritized Codecs
            StringCodec.INSTANCE,
            BinaryCodec.INSTANCE,

            BooleanCodec.INSTANCE,
            ByteCodec.INSTANCE,
            ShortCodec.INSTANCE,
            FloatCodec.INSTANCE,
            DoubleCodec.INSTANCE,
            IntegerCodec.INSTANCE,
            LongCodec.INSTANCE,
            BigIntegerCodec.INSTANCE,
            LocalTimeCodec.INSTANCE,
            LocalDateCodec.INSTANCE,
            LocalDateTimeCodec.INSTANCE,
            UuidCodec.INSTANCE,
            DecimalCodec.INSTANCE,
            MoneyCodec.INSTANCE,
            TimestampCodec.INSTANCE,
            OffsetDateTimeCodec.INSTANCE,
            ZonedDateTimeCodec.INSTANCE,
            BlobCodec.INSTANCE,
            ClobCodec.INSTANCE
        ).toArray(new Codec[0]);

        this.codecPreferences.put(SqlServerType.BIT, BooleanCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.TINYINT, ByteCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.SMALLINT, ShortCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.INTEGER, IntegerCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.BIGINT, LongCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.REAL, FloatCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.FLOAT, DoubleCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.GUID, UuidCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.NUMERIC, DecimalCodec.INSTANCE);
        this.codecPreferences.put(SqlServerType.DECIMAL, DecimalCodec.INSTANCE);
    }

    @SuppressWarnings({"unchecked", "rawtpes"})
    @Override
    public Encoded encode(ByteBufAllocator allocator, RpcParameterContext context, Object value) {

        Assert.requireNonNull(allocator, "ByteBufAllocator must not be null");
        Assert.requireNonNull(context, "RpcParameterContext must not be null");
        Assert.requireNonNull(value, "Value must not be null");

        Object parameterValue = value;
        SqlServerType serverType;

        if (value instanceof Parameter) {

            Parameter parameter = (Parameter) value;
            parameterValue = parameter.getValue();

            if (parameter.getType() instanceof Type.InferredType && parameterValue == null) {
                return encodeNull(allocator, parameter.getType().getJavaType());
            }

            serverType = getServerType(parameter);

        } else {
            serverType = null;
        }

        if (serverType == null) {

            for (Codec<?> codec : this.codecs) {
                if (codec.canEncode(parameterValue)) {
                    return ((Codec) codec).encode(allocator, context, parameterValue);
                }
            }
        } else {

            if (parameterValue == null) {

                for (Codec<?> codec : this.codecs) {
                    if (codec.canEncodeNull(serverType)) {
                        return codec.encodeNull(allocator, serverType);
                    }
                }
            } else {

                for (Codec<?> codec : this.codecs) {
                    if (codec.canEncode(parameterValue)) {
                        return ((Codec) codec).encode(allocator, context.withServerType(serverType), parameterValue);
                    }
                }
            }

        }

        throw new IllegalArgumentException(String.format("Cannot encode [%s] parameter of type [%s]", parameterValue, value.getClass().getName()));
    }

    @Nullable
    private SqlServerType getServerType(Parameter parameter) {

        if (parameter.getType() instanceof Type.InferredType) {
            return null;
        }

        if (parameter.getType() instanceof R2dbcType) {
            return SqlServerType.of((R2dbcType) parameter.getType());
        }

        if (parameter.getType() instanceof SqlServerType) {
            return (SqlServerType) parameter.getType();
        }

        return SqlServerType.of(parameter.getType().getName());
    }

    @Override
    public Encoded encodeNull(ByteBufAllocator allocator, Class<?> type) {

        Assert.requireNonNull(allocator, "ByteBufAllocator must not be null");
        Assert.requireNonNull(type, "Type must not be null");

        Codec<?> codecToUse = this.codecNullCache.computeIfAbsent(type, key -> {

            for (Codec<?> codec : this.codecs) {
                if (codec.canEncodeNull(key)) {
                    return codec;
                }
            }

            throw new IllegalArgumentException(String.format("Cannot encode [null] parameter of type [%s]", type.getName()));

        });

        return codecToUse.encodeNull(allocator);
    }

    @Override
    public <T> T decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends T> type) {

        Assert.requireNonNull(decodable, "Decodable must not be null");
        Assert.requireNonNull(type, "Type must not be null");

        if (buffer == null) {
            return null;
        }

        Codec<T> codec = getDecodingCodec(decodable, type);
        return doDecode(codec, buffer, decodable, type);
    }

    @Nullable
    private <T> T doDecode(Codec<T> codec, @Nullable ByteBuf buffer, Decodable decodable, Class<? extends T> type) {
        return codec.decode(buffer, decodable, type);
    }

    @Override
    public Class<?> getJavaType(TypeInformation type) {

        Assert.requireNonNull(type, "Type must not be null");
        Codec<Object> decodingCodec = getDecodingCodec(new TypeInformationWrapper(type), Object.class);
        return decodingCodec.getType();
    }

    @SuppressWarnings("unchecked")
    private <T> Codec<T> getDecodingCodec(Decodable decodable, Class<? extends T> requestedType) {

        Codec<?> preferredCodec = this.codecPreferences.get(decodable.getType().getServerType());
        if (preferredCodec != null && preferredCodec.canDecode(decodable, requestedType)) {
            return (Codec<T>) preferredCodec;
        }

        for (Codec<?> codec : this.codecs) {
            if (codec.canDecode(decodable, requestedType)) {
                return (Codec<T>) codec;
            }
        }

        throw new IllegalArgumentException(String.format("Cannot decode value of type [%s], name [%s] server type [%s]", requestedType.getName(), decodable.getName(),
            decodable.getType().getServerType()));
    }

    static class TypeInformationWrapper implements Decodable {

        private final TypeInformation typeInformation;

        TypeInformationWrapper(TypeInformation typeInformation) {
            this.typeInformation = typeInformation;
        }

        @Override
        public TypeInformation getType() {
            return this.typeInformation;
        }

        @Override
        public String getName() {
            return this.typeInformation.getServerTypeName();
        }

    }

}
