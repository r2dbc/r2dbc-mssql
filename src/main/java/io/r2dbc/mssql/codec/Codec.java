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
import io.r2dbc.mssql.message.type.TypeInformation;
import reactor.util.annotation.Nullable;

/**
 * Codec to encode and decode values based on Server types and Java value types.<p/>
 * Codecs can decode one or more {@link TypeInformation.SqlServerType server-specific data types} and represent them as a specific Java {@link Class type}. The type parameter of {@link Codec}
 * indicates the interchange type that is handled by this codec.
 * <p/>
 * <p>
 * Codecs that can decode various types (e.g. {@literal uniqueidentifier} and {@literal char}) use the most appropriate method to represent the value by casting or using the
 * {@link Object#toString() toString} method.
 *
 * @param <T> the type that is handled by this codec.
 * @see TypeInformation
 * @see TypeInformation.SqlServerType
 */
interface Codec<T> {

    /**
     * Determine whether this {@link Codec} is capable of decoding a value for the given {@link Decodable} and whether it can represent the decoded value as the desired {@link Class type}.
     * {@link Decodable} represents typically a column or RPC return value.
     *
     * @param decodable the decodable metadata.
     * @param type   the desired value type.
     * @return {@literal true} if this codec is able to decode values of {@link TypeInformation}.
     */
    boolean canDecode(Decodable decodable, Class<?> type);

    /**
     * Decode the {@link ByteBuf data} and return it as the requested {@link Class type}.
     *
     * @param buffer    the data buffer.
     * @param decodable      the decodable descriptor.
     * @param valueType the desired value type.
     * @return the decoded value. Can be {@literal null} if the value is {@literal null}.
     */
    @Nullable
    T decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends T> type);

    /**
     * Encode the value.
     *
     * @param allocator
     * @param typeInformation
     * @param value
     * @return
     */
    ByteBuf encode(ByteBufAllocator allocator, TypeInformation typeInformation, T value);
}
