/*
 * Copyright 2018-2022 the original author or authors.
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
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;

/**
 * Codec for floating-point values that are represented as {@link Double}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#FLOAT} (8-byte) and  {@link SqlServerType#REAL} (4-byte)</li>
 * <li>Java type: {@link Double}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class DoubleCodec extends AbstractCodec<Double> {

    public static final DoubleCodec INSTANCE = new DoubleCodec();

    private static final byte[] NULL = ByteArray.fromEncoded((alloc) -> RpcEncoding.encodeNull(alloc, SqlServerType.FLOAT));

    private DoubleCodec() {
        super(Double.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Double value) {
        return RpcEncoding.encodeFixed(allocator, SqlServerType.FLOAT, value, Encode::asDouble);
    }

    @Override
    Encoded doEncodeNull(ByteBufAllocator allocator) {
        return RpcEncoding.wrap(NULL, SqlServerType.FLOAT);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType() == SqlServerType.FLOAT || typeInformation.getServerType() == SqlServerType.REAL;
    }

    @Override
    Double doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends Double> valueType) {

        if (length.isNull()) {
            return null;
        }

        if (length.getLength() == 4) {
            return (double) Decode.asFloat(buffer);
        }

        return Decode.asDouble(buffer);
    }

}
