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

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.SqlServerType;

/**
 * Codec for numeric values that are represented as {@link Long}.
 *
 * <ul>
 * <li>Server types: Integer numbers</li>
 * <li>Java type: {@link Long}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class LongCodec extends AbstractNumericCodec<Long> {

    /**
     * Singleton instance.
     */
    static final LongCodec INSTANCE = new LongCodec();

    private static final byte[] NULL = ByteArray.fromEncoded((alloc) -> RpcEncoding.encodeNull(alloc, SqlServerType.BIGINT));

    private LongCodec() {
        super(Long.class, value -> value);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Long value) {
        return RpcEncoding.encodeFixed(allocator, SqlServerType.BIGINT, value, Encode::bigint);
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.wrap(NULL, SqlServerType.BIGINT);
    }
}
