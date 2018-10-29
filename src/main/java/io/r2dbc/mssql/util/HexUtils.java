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

package io.r2dbc.mssql.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.util.Objects;

/**
 * Utilities for working with {@link ByteBuf}s.
 */
public final class HexUtils {

    private HexUtils() {
    }

    /**
     * Decode a {@link String} containing Hex-encoded bytes into a {@link ByteBuf}.
     *
     * @param sequence the {@link String} to decode
     * @return the {@link ByteBuf} decoded from the {@link String}
     */
    public static ByteBuf decodeToByteBuf(String chars) {

        Objects.requireNonNull(chars, "String must not be null");

        return Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(chars.replaceAll(" ", "")));
    }
}
