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
import io.r2dbc.mssql.client.ProtocolException;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.message.type.TypeInformation.SqlServerType;

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
    public static final int BIG_MONEY_LENGTH = 8;

    /**
     * Value length of {@link SqlServerType#SMALLMONEY}.
     */
    public static final int SMALL_MONEY_LENGTH = 4;

    private MoneyCodec() {
        super(BigDecimal.class);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType() == SqlServerType.MONEY;
    }

    @Override
    BigDecimal doDecode(ByteBuf buffer, LengthDescriptor length, TypeInformation type, Class<? extends BigDecimal> valueType) {

        BigInteger decoded = decode(buffer, length.getLength());

        return new BigDecimal(decoded, 4);
    }

    private BigInteger decode(ByteBuf buffer, int length) {

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

    @Override
    Encoded doEncode(ByteBufAllocator allocator, TypeInformation type, BigDecimal value) {

        ByteBuf buffer = allocator.buffer(type.getServerType() == SqlServerType.SMALLMONEY ? SMALL_MONEY_LENGTH : BIG_MONEY_LENGTH);

        if (type.getServerType() == SqlServerType.SMALLMONEY) {

            BigInteger bigInteger = value.unscaledValue();
            Encode.smallMoney(buffer, bigInteger);
        }

        return null;
    }
}
