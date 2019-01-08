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
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Abstract codec class that provides a basis for  concrete
 * implementations of a {@link Codec} for integer numeric data types.
 *
 * @author Mark Paluch
 */
abstract class AbstractNumericCodec<T> extends AbstractCodec<T> {

    /**
     * Length in bytes required to represent {@literal BIGINT}.
     */
    static final int SIZE_BIGINT = 8;

    /**
     * Length in bytes required to represent {@literal INT}.
     */
    static final int SIZE_INT = 4;

    /**
     * Length in bytes required to represent {@literal SMALLINT}.
     */
    static final int SIZE_SMALL_INT = 2;

    /**
     * Length in bytes required to represent {@literal TINYINT}.
     */
    static final int SIZE_TINY_INT = 1;

    private static final Set<SqlServerType> SUPPORTED_TYPES = EnumSet.of(SqlServerType.BIT, SqlServerType.TINYINT, SqlServerType.SMALLINT, SqlServerType.INTEGER, SqlServerType.BIGINT);

    private final LongToObjectFunction<T> converter;

    AbstractNumericCodec(Class<T> type, LongToObjectFunction<T> converter) {
        super(type);
        this.converter = converter;
    }

    @Override
    boolean doCanDecode(TypeInformation typeInformation) {
        return SUPPORTED_TYPES.contains(typeInformation.getServerType());
    }

    @Override
    T doDecode(ByteBuf buffer, Length length, TypeInformation typeInformation, Class<? extends T> valueType) {

        if (length.isNull()) {
            return null;
        }

        switch (length.getLength()) {
            case SIZE_BIGINT:
                return this.converter.apply(Decode.bigint(buffer));
            case SIZE_INT:
                return this.converter.apply(Decode.asInt(buffer));
            case SIZE_SMALL_INT:
                return this.converter.apply(Decode.smallInt(buffer));
            case SIZE_TINY_INT:
                return this.converter.apply(Decode.tinyInt(buffer));
            default:
                throw ProtocolException.invalidTds(String.format("Unexpected value length: %d", length.getLength()));
        }
    }

    /**
     * Represents a function that produces a object-valued result.
     *
     * @param <T> the type of the input to the function
     * @see Function
     */
    @FunctionalInterface
    interface LongToObjectFunction<T> {

        /**
         * Applies this function to the given argument.
         *
         * @param value the function argument
         * @return the function result
         */
        T apply(long value);
    }
}
