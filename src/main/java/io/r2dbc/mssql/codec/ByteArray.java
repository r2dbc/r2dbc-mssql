/*
 * Copyright 2019-2021 the original author or authors.
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
import io.netty.buffer.ByteBufUtil;
import io.r2dbc.mssql.util.Assert;

import java.util.function.Function;

/**
 * Utility to create byte arrays.
 *
 * @author Mark Paluch
 */
abstract class ByteArray {

    /**
     * Create a {@code byte[]} from a {@link Function encode function} accepting {@link ByteBufAllocator} returning {@link Encoded}.
     *
     * @param encodeFunction encode function.
     * @return the {@code byte[]} containing the encoded buffer.
     */
    static byte[] fromEncoded(Function<ByteBufAllocator, Encoded> encodeFunction) {

        Assert.notNull(encodeFunction, "Encode Function must not be null");

        Encoded encoded = encodeFunction.apply(ByteBufAllocator.DEFAULT);

        try {
            return ByteBufUtil.getBytes(encoded.getValue());
        } finally {
            encoded.release();
        }
    }

    /**
     * Create a {@code byte[]} from a {@link Function encode function} accepting {@link ByteBufAllocator} returning {@link ByteBuf}.
     *
     * @param encodeFunction encode function.
     * @return the {@code byte[]} containing the encoded buffer.
     */
    static byte[] fromBuffer(Function<ByteBufAllocator, ByteBuf> encodeFunction) {

        ByteBuf buffer = encodeFunction.apply(ByteBufAllocator.DEFAULT);

        try {
            return ByteBufUtil.getBytes(buffer);
        } finally {
            buffer.release();
        }
    }

    // Utility constructor
    private ByteArray() {
    }

}
