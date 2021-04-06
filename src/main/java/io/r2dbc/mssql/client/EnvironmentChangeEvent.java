/*
 * Copyright 2018-2021 the original author or authors.
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

import io.r2dbc.mssql.message.token.EnvChangeToken;
import io.r2dbc.mssql.util.Assert;

/**
 * Environment change event based on a {@link EnvChangeToken}.
 *
 * @author Mark Paluch
 */
public class EnvironmentChangeEvent {

    private final EnvChangeToken token;

    /**
     * Create a new {@link EnvironmentChangeEvent}.
     *
     * @param token the environment change token.
     */
    public EnvironmentChangeEvent(EnvChangeToken token) {
        this.token = Assert.requireNonNull(token, "EnvChangeToken must not be null");
    }

    /**
     * @return the environment change token.
     */
    public EnvChangeToken getToken() {
        return this.token;
    }

}
