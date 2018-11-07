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
import io.r2dbc.mssql.message.type.TdsDataType;

/**
 * Codec for numeric values that are represented as {@link Boolean}.
 *
 * <ul>
 * <li>Server types: Integer numbers</li>
 * <li>Java type: {@link Boolean}</li>
 * <li>Downcast: {@literal true} if the value  is not zero</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class BooleanCodec extends AbstractNumericCodec<Boolean> {

    /**
     * Singleton instance.
     */
    static final BooleanCodec INSTANCE = new BooleanCodec();

    private BooleanCodec() {
        super(Boolean.class, value -> value != 0);
    }


    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Boolean value) {
        return RpcEncoding.encode(allocator, TdsDataType.BIT1, value, (buffer, b) -> Encode.asByte(buffer, b ? 1 : 0));
    }
}
