/*
 * Copyright 2018-2019 the original author or authors.
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
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;

import java.util.EnumSet;
import java.util.Set;

/**
 * Codec for character values that are represented as {@link String}.
 *
 * <ul>
 * <li>Server types: (N)(VAR)CHAR</li>
 * <li>Java type: {@link Enum}</li>
 * <li>Downcast: to {@link Enum}</li>
 * </ul>
 *
 * @author Stephan Dreyer
 */
@SuppressWarnings("rawtypes")
final class EnumCodec extends AbstractCodec<Enum> {

    private final StringCodec delegate = StringCodec.INSTANCE;

    /**
     * Singleton instance.
     */
    public static final EnumCodec INSTANCE = new EnumCodec();

    private static final Set<SqlServerType> SUPPORTED_TYPES = EnumSet.of(SqlServerType.CHAR, SqlServerType.NCHAR, SqlServerType.NVARCHAR, SqlServerType.VARCHAR);

    private EnumCodec() {
        super(Enum.class);
    }

    @Override
    Encoded doEncode(ByteBufAllocator allocator, RpcParameterContext context, Enum value) {
        Assert.requireNonNull(value, "value must not be null");
        return delegate.doEncode(allocator, context, value.name());
    }

    @Override
    public Encoded doEncodeNull(ByteBufAllocator allocator) {
        return delegate.doEncodeNull(allocator);
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return SUPPORTED_TYPES.contains(typeInformation.getServerType());
    }

    @Override
    @SuppressWarnings("unchecked")
    Enum doDecode(ByteBuf buffer, Length length, TypeInformation typeInformation, Class<? extends Enum> valueType) {
        if (length.isNull()) {
            return null;
        }

        return Enum.valueOf(valueType, delegate.doDecode(buffer, length, typeInformation, String.class).trim());
    }
}
