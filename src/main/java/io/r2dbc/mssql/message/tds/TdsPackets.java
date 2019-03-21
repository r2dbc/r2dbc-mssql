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

package io.r2dbc.mssql.message.tds;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.HeaderOptions;

/**
 * Factory class for {@link TdsFragment} instances.
 *
 * @author Mark Paluch
 */
public interface TdsPackets {

    /**
     * Create a contextual TDS fragment.
     *
     * @param options the {@link HeaderOptions} to apply for the final {@link Header} creation.
     * @param buffer  the TDS message.
     * @return the {@link ContextualTdsFragment}.
     */
    static ContextualTdsFragment create(HeaderOptions options, ByteBuf buffer) {
        return new ContextualTdsFragment(options, buffer);
    }

    /**
     * Create a TDS fragment.
     *
     * @param buffer
     * @return the {@link TdsFragment}.
     */
    static TdsFragment create(ByteBuf buffer) {
        return new TdsFragment(buffer);
    }

    /**
     * Create a first contextual TDS fragment.
     *
     * @param options header options to form an actual {@link Header} during encoding.
     * @param buffer  the TDS message.
     * @return the {@link ContextualTdsFragment}.
     */
    static FirstTdsFragment first(HeaderOptions options, ByteBuf buffer) {
        return new FirstTdsFragment(options, buffer);
    }

    /**
     * Create a last contextual TDS fragment.
     *
     * @param buffer the TDS message.
     * @return the {@link ContextualTdsFragment}.
     */
    static LastTdsFragment last(ByteBuf buffer) {
        return new LastTdsFragment(buffer);
    }

    /**
     * Create a self-contained TDS Packet.
     *
     * @param header the TDS header.
     * @param buffer the TDS message.
     * @return the {@link TdsPacket}.
     */
    static TdsPacket create(Header header, ByteBuf buffer) {
        return new TdsPacket(header, buffer);
    }
}
