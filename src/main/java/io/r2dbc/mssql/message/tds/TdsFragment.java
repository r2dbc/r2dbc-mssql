/*
 * Copyright 2018 the original author or authors.
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
import io.r2dbc.mssql.util.Assert;

/**
 * Fragment of a TDS message. Can be a partial or complete TDS message. Headers are supplied during encoding.
 *
 * @author Mark Paluch
 */
public class TdsFragment {

    private final ByteBuf byteBuf;

    /**
     * Create a TDS fragment.
     *
     * @param buffer the data buffer.
     */
    TdsFragment(ByteBuf buffer) {
        this.byteBuf = Assert.requireNonNull(buffer, "Data buffer must not be null");
    }

    /**
     * @return the data buffer.
     */
    public ByteBuf getByteBuf() {
        return this.byteBuf;
    }
}
