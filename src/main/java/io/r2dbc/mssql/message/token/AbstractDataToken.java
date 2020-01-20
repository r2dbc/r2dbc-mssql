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

package io.r2dbc.mssql.message.token;

/**
 * Data token. A data token is typed by a single byte and provides a symbolic name.
 *
 * @author Mark Paluch
 */
public abstract class AbstractDataToken implements DataToken {

    private final byte type;

    /**
     * Creates a new {@link AbstractDataToken}.
     *
     * @param type token type.
     */
    protected AbstractDataToken(byte type) {
        this.type = type;
    }

    /**
     * @return the token type.
     */
    public byte getType() {
        return this.type;
    }

    /**
     * @return symbolic name of the {@link AbstractDataToken}.
     */
    public abstract String getName();
}
