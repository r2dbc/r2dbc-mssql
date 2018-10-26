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

/**
 * Registry for {@link Codec}s to encodes and decodes values.
 */
public interface Codecs {

    /**
     * Decode a data to a value.
     *
     * @param buffer the {@link ByteBuf} to decode
     * @param type   the type to decode to
     * @param <T>    the type of item being returned
     * @return the decoded value
     */
    @Nullable
    <T> T decode(@Nullable ByteBuf buffer, Column column, Class<? extends T> type);

}
