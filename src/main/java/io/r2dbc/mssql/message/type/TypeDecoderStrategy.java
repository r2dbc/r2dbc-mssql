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

package io.r2dbc.mssql.message.type;

import io.netty.buffer.ByteBuf;

/**
 * Interface typically implemented by type decoder implementations that can decode a {@link TypeInformation}.
 */
public interface TypeDecoderStrategy {

    /**
     * Check whether the {@link ByteBuf} contains sufficient readable bytes to decode the {@link TypeInformation}.
     *
     * @param buffer the data buffer.
     * @return {@literal true} if the data buffer contains sufficient readable bytes to decode the {@link TypeInformation}.
     */
    boolean canDecode(ByteBuf buffer);

    /**
     * Decode the type information and enhance {@link MutableTypeInformation}.
     *
     * @param typeInfo the mutable {@link TypeInformation}.
     * @param buffer   the data buffer.
     */
    void decode(MutableTypeInformation typeInfo, ByteBuf buffer);
}
