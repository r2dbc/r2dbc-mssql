/*
 * Copyright 2018-2021 the original author or authors.
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
import reactor.util.annotation.Nullable;

/**
 * Codec to encode and decode values based on Server types and Java value types.<p/>
 * Codecs can decode one or more {@link SqlServerType server-specific data types} and represent them as a specific Java {@link Class type}. The type parameter of {@link Codec}
 * indicates the interchange type that is handled by this codec.
 * <p/>
 * Codecs that can decode various types (e.g. {@literal uniqueidentifier} and {@literal char}) use the most appropriate method to represent the value by casting or using the
 * {@link Object#toString() toString} method.
 *
 * @param <T> the type that is handled by this codec.
 * @see TypeInformation
 * @see SqlServerType
 */
interface Codec<T> {

    /**
     * Determine whether this {@link Codec} is capable of encoding the {@code value}.
     *
     * @param value the parameter value.
     * @return {@code true} if this {@link Codec} is able to encode the {@code value}.
     * @see #encodeNull
     */
    boolean canEncode(Object value);

    /**
     * Encode the {@code value} to be used as RPC parameter.
     *
     * @param allocator the allocator to allocate encoding buffers.
     * @param context   parameter context.
     * @param value     the {@code null} {@code value}.
     * @return the encoded value.
     */
    Encoded encode(ByteBufAllocator allocator, RpcParameterContext context, T value);

    /**
     * Determine whether this {@link Codec} is capable of encoding a {@code null} value for the given {@link Class} type.
     *
     * @param type the desired value type.
     * @return {@code true} if this {@link Codec} is able to encode {@code null} values for the given {@link Class} type.
     * @see #encodeNull
     */
    boolean canEncodeNull(Class<?> type);

    /**
     * Encode a {@code null} value.
     *
     * @param allocator the allocator to allocate encoding buffers.
     * @return the encoded {@code null} value.
     */
    Encoded encodeNull(ByteBufAllocator allocator);

    /**
     * Determine whether this {@link Codec} is capable of decoding a value for the given {@link Decodable} and whether it can represent the decoded value as the desired {@link Class type}.
     * {@link Decodable} represents typically a column or RPC return value.
     *
     * @param decodable the decodable metadata.
     * @param type      the desired value type.
     * @return {@code true} if this codec is able to decode values of {@link TypeInformation}.
     */
    boolean canDecode(Decodable decodable, Class<?> type);

    /**
     * Decode the {@link ByteBuf data} and return it as the requested {@link Class type}.
     *
     * @param buffer    the data buffer.
     * @param decodable the decodable descriptor.
     * @param type      the desired value type.
     * @return the decoded value. Can be {@code null} if the value is {@code null}.
     */
    @Nullable
    T decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends T> type);

    /**
     * Returns the Java {@link Class type} of this codec.
     *
     * @return the Java type.
     */
    Class<T> getType();

}
