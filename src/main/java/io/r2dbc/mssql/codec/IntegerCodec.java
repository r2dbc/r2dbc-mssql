/*
 * Copyright 2018 the original author or authors.
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
 * Codec for numeric values that are represented as {@link Integer}.
 *
 * <ul>
 * <li>Server types: Integer numbers</li>
 * <li>Java type: {@link Integer}</li>
 * <li>Downcast: to {@link Integer}</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class IntegerCodec extends AbstractNumericCodec<Integer> {

    /**
     * Singleton instance.
     */
    static final IntegerCodec INSTANCE = new IntegerCodec();

    private IntegerCodec() {
        super(Integer.class, value -> (int) value);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Integer value) {
        return RpcEncoding.encodeFixed(allocator, SqlServerType.INTEGER, value, Encode::asInt);
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.encodeNull(allocator, SqlServerType.INTEGER);
    }
}
