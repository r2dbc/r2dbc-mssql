/*
 * Copyright 2018-2022 the original author or authors.
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

package io.r2dbc.mssql;

import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;

/**
 * A strongly typed implementation of {@link Statement} for a Microsoft SQL Server database.
 * <p>
 * Microsoft SQL Server uses named parameters for parametrized statements:
 * <pre class="code">
 * INSERT INTO person (id, first_name, last_name) VALUES(@id, @firstname, @lastname)
 * </pre>
 * Use {@link #bind(String, Object)} and {@link #bindNull(String, Class)} over positional ({@link #bind(int, Object)}) binding.
 *
 * @author Mark Paluch
 */
public interface MssqlStatement extends Statement {

    /**
     * {@inheritDoc}
     */
    @Override
    MssqlStatement add();

    /**
     * {@inheritDoc}
     */
    @Override
    MssqlStatement bind(int index, Object value);

    /**
     * {@inheritDoc}
     */
    @Override
    MssqlStatement bindNull(int index, Class<?> type);

    /**
     * {@inheritDoc}
     */
    @Override
    MssqlStatement bind(String identifier, Object value);

    /**
     * {@inheritDoc}
     */
    @Override
    MssqlStatement bindNull(String identifier, Class<?> type);

    /**
     * {@inheritDoc}
     */
    @Override
    Flux<MssqlResult> execute();

    /**
     * {@inheritDoc}
     */
    @Override
    MssqlStatement returnGeneratedValues(String... columns);

    /**
     * {@inheritDoc}
     */
    @Override
    MssqlStatement fetchSize(int fetchSize);

}
