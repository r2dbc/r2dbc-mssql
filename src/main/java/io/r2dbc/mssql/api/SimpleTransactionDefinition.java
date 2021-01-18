/*
 * Copyright 2021 the original author or authors.
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

package io.r2dbc.mssql.api;

import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
final class SimpleTransactionDefinition implements MssqlTransactionDefinition {

    public static final SimpleTransactionDefinition EMPTY = new SimpleTransactionDefinition(Collections.emptyMap());

    private final Map<Option<?>, Object> options;

    SimpleTransactionDefinition(Map<Option<?>, Object> options) {
        this.options = options;
    }

    @Override
    public <T> T getAttribute(Option<T> option) {
        return (T) this.options.get(option);
    }

    public MssqlTransactionDefinition with(Option<?> option, Object value) {

        Map<Option<?>, Object> options = new HashMap<>(this.options);
        options.put(Assert.requireNonNull(option, "option must not be null"), Assert.requireNonNull(value, "value must not be null"));

        return new SimpleTransactionDefinition(options);
    }

    @Override
    public MssqlTransactionDefinition isolationLevel(IsolationLevel isolationLevel) {
        return with(MssqlTransactionDefinition.ISOLATION_LEVEL, isolationLevel);
    }

    @Override
    public MssqlTransactionDefinition lockTimeout(Duration timeout) {
        return with(MssqlTransactionDefinition.LOCK_WAIT_TIMEOUT, timeout);
    }

    @Override
    public MssqlTransactionDefinition name(String name) {
        return with(MssqlTransactionDefinition.NAME, name);
    }

    @Override
    public MssqlTransactionDefinition mark(String mark) {
        if (getAttribute(TransactionDefinition.NAME) == null) {
            name(mark);
        }

        return with(MARK, mark);
    }

}
