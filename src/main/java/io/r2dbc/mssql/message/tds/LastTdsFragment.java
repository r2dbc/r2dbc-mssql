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
import io.r2dbc.mssql.message.header.HeaderOptions;

/**
 * Last chunk of a TDS message. This message type signals the encoder send this fragment with the previously associated
 * {@link HeaderOptions} and clear these after sending this message.
 *
 * @author Mark Paluch
 * @see FirstTdsFragment
 */
public final class LastTdsFragment extends TdsFragment {

    /**
     * Creates a new {@link LastTdsFragment}.
     *
     * @param byteBuf       the buffer.
     * @param headerOptions header options.
     */
    LastTdsFragment(ByteBuf byteBuf) {
        super(byteBuf);
    }
}
