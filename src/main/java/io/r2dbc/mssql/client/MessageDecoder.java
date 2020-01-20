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

package io.r2dbc.mssql.client;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.Header;
import reactor.core.publisher.SynchronousSink;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Decoder interface that accepts a {@link Header} and {@link ByteBuf data buffer} to attempt to decode {@link Message}s.
 *
 * @author Mark Paluch
 * @see StreamDecoder
 */
interface MessageDecoder extends BiFunction<Header, ByteBuf, List<? extends Message>> {

    /**
     * Apply the decoder function {@link #decode(Header, ByteBuf, SynchronousSink)} and notify {@link SynchronousSink} about every decoded {@link Message}.
     *
     * @param header
     * @param buffer
     * @param sink
     * @return
     */
    default boolean decode(Header header, ByteBuf buffer, SynchronousSink<Message> sink) {

        List<? extends Message> messages = apply(header, buffer);

        if (messages.isEmpty()) {
            return false;
        }

        messages.forEach(sink::next);
        return true;
    }
}
