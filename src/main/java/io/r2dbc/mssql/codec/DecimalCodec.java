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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Codec for fixed floating-point values that are represented as {@link BigDecimal}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#NUMERIC} and  {@link SqlServerType#DECIMAL}</li>
 * <li>Java type: {@link BigDecimal}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class DecimalCodec extends AbstractCodec<BigDecimal> {

    static final DecimalCodec INSTANCE = new DecimalCodec();

    private DecimalCodec() {
        super(BigDecimal.class);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType() == TypeInformation.SqlServerType.DECIMAL || typeInformation.getServerType() == TypeInformation.SqlServerType.NUMERIC;
    }

    @Override
    BigDecimal doDecode(ByteBuf buffer, LengthDescriptor length, TypeInformation type, Class<? extends BigDecimal> valueType) {

        byte signByte = buffer.readByte();
        int sign = (0 == signByte) ? -1 : 1;
        byte[] magnitude = new byte[length.getLength() - 1];

        // read magnitude LE
        for (int i = 0; i < magnitude.length; i++) {
            magnitude[magnitude.length - 1 - i] = buffer.readByte();
        }

        return new BigDecimal(new BigInteger(sign, magnitude), type.getScale());
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, TypeInformation type, BigDecimal value) {
        return null;
    }
}
