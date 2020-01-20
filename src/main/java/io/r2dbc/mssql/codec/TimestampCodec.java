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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;

/**
 * Codec for binary timestamp values that are represented as {@code byte[]}.
 *
 * <ul>
 * <li>Server types: {@link SqlServerType#TIMESTAMP} (8-byte)</li>
 * <li>Java type: {@code byte[]}</li>
 * <li>Downcast: none</li>
 * </ul>
 *
 * @author Mark Paluch
 */
final class TimestampCodec extends AbstractCodec<byte[]> {

    static final TimestampCodec INSTANCE = new TimestampCodec();

    private TimestampCodec() {
        super(byte[].class);
    }

    @Override
    public boolean canEncode(Object value) {
        return false;
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Encoded doEncodeNull(ByteBufAllocator allocator) {
        throw new UnsupportedOperationException();
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return typeInformation.getServerType() == SqlServerType.TIMESTAMP;
    }

    @Override
    byte[] doDecode(ByteBuf buffer, Length length, TypeInformation type, Class<? extends byte[]> valueType) {

        byte[] value = new byte[length.getLength()];

        buffer.readBytes(value);

        return value;
    }

}
