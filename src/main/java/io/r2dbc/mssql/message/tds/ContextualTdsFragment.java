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
import io.r2dbc.mssql.util.Assert;

/**
 * Represents a TDS message with associated {@link HeaderOptions}. The encoder may split this packet into multiple
 * packets if the packet size exceeds the negotiated packet size.
 *
 * @author Mark Paluch
 */
public class ContextualTdsFragment extends TdsFragment {

    private final HeaderOptions headerOptions;

    /**
     * Creates a new {@link ContextualTdsFragment}.
     *
     * @param headerOptions header options.
     * @param byteBuf       the buffer.
     */
    public ContextualTdsFragment(HeaderOptions headerOptions, ByteBuf byteBuf) {

        super(byteBuf);
        this.headerOptions = Assert.requireNonNull(headerOptions, "HeaderOptions must not be null");
    }

    /**
     * @return the header options.
     */
    public HeaderOptions getHeaderOptions() {
        return this.headerOptions;
    }
}
