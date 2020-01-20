/*
 * Copyright 2018-2019 the original author or authors.
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
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Codec for numeric values that are represented as {@link BigInteger}.
 *
 * <ul>
 * <li>Server types: Integer numbers</li>
 * <li>Java type: {@link BigInteger}</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class BigIntegerCodec extends AbstractNumericCodec<BigInteger> {

    /**
     * Singleton instance.
     */
    static final BigIntegerCodec INSTANCE = new BigIntegerCodec();

    private static final byte[] NULL = ByteArray.fromEncoded((alloc) -> RpcEncoding.encodeNull(alloc, SqlServerType.BIGINT));

    private BigIntegerCodec() {
        super(BigInteger.class, BigInteger::valueOf);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, BigInteger value) {
        return DecimalCodec.INSTANCE.encode(allocator, context, new BigDecimal(value));
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.wrap(NULL, SqlServerType.BIGINT);
    }

    @Override
    BigInteger doDecode(ByteBuf buffer, Length length, TypeInformation typeInformation, Class<? extends BigInteger> valueType) {

        if (typeInformation.getServerType() == SqlServerType.NUMERIC || typeInformation.getServerType() == SqlServerType.DECIMAL) {
            BigDecimal decimal = DecimalCodec.INSTANCE.doDecode(buffer, length, typeInformation, BigDecimal.class);

            if (decimal == null) {
                return null;
            }

            return decimal.toBigInteger();
        }

        return super.doDecode(buffer, length, typeInformation, valueType);
    }
}
