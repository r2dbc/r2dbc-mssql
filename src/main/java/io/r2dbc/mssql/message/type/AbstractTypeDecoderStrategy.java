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

package io.r2dbc.mssql.message.type;

import io.netty.buffer.ByteBuf;

/**
 * Support class for {@link TypeDecoderStrategy} implementations.
 *
 * @author Mark Paluch
 */
abstract class AbstractTypeDecoderStrategy implements TypeDecoderStrategy {

    private final int typeDescriptorLength;

    /**
     * Creates a new {@link AbstractTypeDecoderStrategy}.
     *
     * @param typeDescriptorLength length in bytes.
     */
    AbstractTypeDecoderStrategy(int typeDescriptorLength) {
        this.typeDescriptorLength = typeDescriptorLength;
    }

    @Override
    public final boolean canDecode(ByteBuf buffer) {
        return buffer.readableBytes() >= this.typeDescriptorLength;
    }
}
