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

package io.r2dbc.mssql.message.header;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Provider for the PacketId {@link Header} field.
 *
 * @author Mark Paluch
 * @see Header#getPacketId()
 * @see Header#encode(ByteBuf, PacketIdProvider)
 */
@FunctionalInterface
public interface PacketIdProvider {

    /**
     * @return the next packet id.
     */
    byte nextPacketId();

    /**
     * Static packet Id provider.
     *
     * @param value packet number.
     * @return {@link PacketIdProvider} returning the static {@code value}.
     */
    static PacketIdProvider just(int value) {
        return just((byte) (value % 256));
    }

    /**
     * Static packet Id provider.
     *
     * @param value packet number.
     * @return {@link PacketIdProvider} returning the static {@code value}.
     */
    static PacketIdProvider just(byte value) {
        return () -> value;
    }

    /**
     * Atomic/concurrent packetId provider.
     *
     * @return a thread-safe packet counter.
     */
    static PacketIdProvider atomic() {

        AtomicLong counter = new AtomicLong();
        return () -> (byte) (counter.incrementAndGet() % 256);
    }
}
