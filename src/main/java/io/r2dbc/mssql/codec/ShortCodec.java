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

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.SqlServerType;

/**
 * Codec for numeric values that are represented as {@link Short}.
 *
 * <ul>
 * <li>Server types: Integer numbers</li>
 * <li>Java type: {@link Short}</li>
 * <li>Downcast: to {@link Short}</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class ShortCodec extends AbstractNumericCodec<Short> {

    /**
     * Singleton instance.
     */
    static final ShortCodec INSTANCE = new ShortCodec();

    private ShortCodec() {
        super(Short.class, value -> (short) value);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Short value) {
        return RpcEncoding.encodeFixed(allocator, SqlServerType.SMALLINT, value, Encode::smallInt);
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.encodeNull(allocator, SqlServerType.SMALLINT);
    }
}
