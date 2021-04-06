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

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.SqlServerType;

/**
 * Codec for numeric values that are represented as {@link Byte}.
 *
 * <ul>
 * <li>Server types: Integer numbers</li>
 * <li>Java type: {@link Byte}</li>
 * <li>Downcast: to {@link Byte}</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class ByteCodec extends AbstractNumericCodec<Byte> {

    /**
     * Singleton instance.
     */
    static final ByteCodec INSTANCE = new ByteCodec();

    private static final byte[] NULL = ByteArray.fromEncoded((alloc) -> RpcEncoding.encodeNull(alloc, SqlServerType.TINYINT));

    private ByteCodec() {
        super(Byte.class, value -> (byte) value);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Byte value) {
        return RpcEncoding.encodeFixed(allocator, SqlServerType.TINYINT, value, Encode::asByte);
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.wrap(NULL, SqlServerType.TINYINT);
    }

}
