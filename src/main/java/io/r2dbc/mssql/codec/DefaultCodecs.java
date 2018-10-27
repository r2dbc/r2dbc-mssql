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
import io.r2dbc.mssql.message.token.Column;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The default {@link Codec} implementation.  Delegates to type-specific codec implementations.
 */
public final class DefaultCodecs implements Codecs {

    private final List<Codec<?>> codecs;

    /**
     * Creates a new instance of {@link DefaultCodecs}.
     */
    public DefaultCodecs() {

        this.codecs = Arrays.asList(

            // Prioritized Codecs
            ShortCodec.INSTANCE,
            StringCodec.INSTANCE,

            BooleanCodec.INSTANCE,
            ByteCodec.INSTANCE,
            FloatCodec.INSTANCE,
            IntegerCodec.INSTANCE,
            LongCodec.INSTANCE,
            UuidCodec.INSTANCE,
            MoneyCodec.INSTANCE
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(@Nullable ByteBuf buffer, Column column, Class<? extends T> type) {

        Objects.requireNonNull(column, "Column must not be null");
        Objects.requireNonNull(type, "Type must not be null");

        if (buffer == null) {
            return null;
        }

        for (Codec<?> codec : this.codecs) {
            if (codec.canDecode(column, type)) {
                return ((Codec<T>) codec).decode(buffer, column, type);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot decode value of type [%s] for column [%s], server type [%s]", type.getName(), column.getName(), column.getType().getServerType()));
    }
}
