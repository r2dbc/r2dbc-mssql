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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Codec for fixed floating-point values that are represented as {@link BigDecimal}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#MONEY} (8-byte) and  {@link SqlServerType#SMALLMONEY} (4-byte)</li>
 * <li>Java type: {@link BigDecimal}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class MoneyCodec extends AbstractCodec<BigDecimal> {

    /**
     * Singleton instance.
     */
    static final MoneyCodec INSTANCE = new MoneyCodec();

    /**
     * Value length of {@link SqlServerType#MONEY}.
     */
    private static final int BIG_MONEY_LENGTH = 8;

    /**
     * Value length of {@link SqlServerType#SMALLMONEY}.
     */
    private static final int SMALL_MONEY_LENGTH = 4;

    private MoneyCodec() {
        super(BigDecimal.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, BigDecimal value) {
        return RpcEncoding.encodeFixed(allocator, SqlServerType.MONEY, value, (buffer, bigDecimal) -> Encode.money(buffer, bigDecimal.unscaledValue()));
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.encodeNull(allocator, SqlServerType.MONEY);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType() == SqlServerType.MONEY;
    }

    @Override
    BigDecimal doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends BigDecimal> valueType) {

        BigInteger decoded = decode(buffer, length.getLength());

        return new BigDecimal(decoded, 4);
    }

    private static BigInteger decode(ByteBuf buffer, int length) {

        switch (length) {
            case BIG_MONEY_LENGTH:

                int intBitsHi = Decode.asInt(buffer);
                int intBitsLo = Decode.asInt(buffer);

                return BigInteger.valueOf(((long) intBitsHi << 32) | (intBitsLo & 0xFFFFFFFFL));

            case SMALL_MONEY_LENGTH:
                return BigInteger.valueOf(Decode.asInt(buffer));

            default:
                throw ProtocolException.invalidTds(String.format("Unexpected value length: %d", length));
        }
    }
}
