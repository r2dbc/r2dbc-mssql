/*
 * Copyright 2018-2019 the original author or authors.
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
import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ReturnValue;
import io.r2dbc.mssql.message.token.RowToken;
import reactor.util.annotation.Nullable;

/**
 * Registry for {@link Codec}s to encode RPC parameters and decode tabular values.
 *
 * @see Codec
 * @see ReturnValue
 * @see Column
 * @see RowToken
 */
public interface Codecs {

    /**
     * Encode a non-{@code null} {@code value} as RPC parameter.
     *
     * @param allocator the allocator to allocate encoding buffers.
     * @param context   parameter context.
     * @param value     the {@code null} {@code value}.
     * @return the encoded value. Must be {@link ReferenceCounted#release() released} after usage.
     */
    Encoded encode(ByteBufAllocator allocator, RpcParameterContext context, Object value);

    /**
     * Encode a {@code null} value for a specific {@link Class type}.
     *
     * @param allocator the allocator to allocate encoding buffers.
     * @param type      the type to represent {@code null}.
     * @return the encoded {@code null} value.
     */
    Encoded encodeNull(ByteBufAllocator allocator, Class<?> type);

    /**
     * Decode a data to a value.
     *
     * @param buffer    the {@link ByteBuf} to decode.
     * @param decodable the decodable metadata.
     * @param type      the type to decode to.
     * @param <T>       the type of item being returned.
     * @return the decoded value. Can be {@code null} if the column value is {@code null}.
     */
    @Nullable
    <T> T decode(@Nullable ByteBuf buffer, Decodable decodable, Class<? extends T> type);
}
