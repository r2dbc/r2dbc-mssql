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

package io.r2dbc.mssql.message.tds;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.header.HeaderOptions;

/**
 * First chunk of a TDS message. This message type signals the encoder to associate subsequent messages with the
 * attached {@link HeaderOptions}.
 *
 * @author Mark Paluch
 * @see ContextualTdsFragment
 * @see LastTdsFragment
 */
public final class FirstTdsFragment extends ContextualTdsFragment {

    /**
     * Creates a new {@link FirstTdsFragment}.
     *
     * @param headerOptions header options.
     * @param byteBuf       the buffer.
     */
    FirstTdsFragment(HeaderOptions headerOptions, ByteBuf byteBuf) {
        super(headerOptions, byteBuf);
    }
}
