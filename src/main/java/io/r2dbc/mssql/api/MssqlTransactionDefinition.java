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

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;

import java.time.Duration;

/**
 * {@link TransactionDefinition} for a SQL Server database.
 *
 * @author Mark Paluch
 * @since 0.9
 */
public interface MssqlTransactionDefinition extends TransactionDefinition {

    /**
     * The {@code WITH MARK} description is a string that marks the transaction in the transaction log and being stored in the {@code msdb.dbo.logmarkhistory} table.
     */
    Option<String> MARK = Option.valueOf("mark");

    /**
     * Creates a {@link MssqlTransactionDefinition} given {@link IsolationLevel}.
     *
     * @param isolationLevel the isolation level to use during the transaction.
     * @return a new {@link MssqlTransactionDefinition} using {@link IsolationLevel}.
     * @throws IllegalArgumentException if {@code isolationLevel} is {@code null}.
     */
    static MssqlTransactionDefinition from(IsolationLevel isolationLevel) {
        return SimpleTransactionDefinition.EMPTY.isolationLevel(isolationLevel);
    }

    /**
     * Creates a {@link MssqlTransactionDefinition} specifying the transaction name.
     *
     * @param name the transaction name. Must not exceed 32 characters. The name is always case sensitive, even when the instance of SQL Server is not case sensitive
     * @return a new {@link MssqlTransactionDefinition} using transaction {@code name}.
     */
    static MssqlTransactionDefinition named(String name) {
        return SimpleTransactionDefinition.EMPTY.name(name);
    }

    /**
     * Creates a {@link MssqlTransactionDefinition} retaining all configured options and applying {@link IsolationLevel}.
     *
     * @param isolationLevel the isolation level to use during the transaction.
     * @return a new {@link MssqlTransactionDefinition} retaining all configured options and applying {@link IsolationLevel}.
     * @throws IllegalArgumentException if {@code isolationLevel} is {@code null}.
     */
    MssqlTransactionDefinition isolationLevel(IsolationLevel isolationLevel);

    /**
     * Creates a {@link MssqlTransactionDefinition} retaining all configured options and applying {@link Duration lock timeout}.
     *
     * @param timeout the lock timeout.
     * @return a new {@link MssqlTransactionDefinition} retaining all configured options and applying {@link Duration lock timeout}.
     * @throws IllegalArgumentException if {@code timeout} is {@code null}.
     */
    MssqlTransactionDefinition lockTimeout(Duration timeout);

    /**
     * Creates a {@link MssqlTransactionDefinition} retaining all configured options and using the given transaction {@code name}.
     *
     * @param name the transaction name. Must not exceed 32 characters. The name is always case sensitive, even when the instance of SQL Server is not case sensitive
     * @return a new {@link MssqlTransactionDefinition} retaining all configured options and using the given transaction {@code name}.
     * @throws IllegalArgumentException if {@code name} is {@code null}.
     */
    MssqlTransactionDefinition name(String name);

    /**
     * Creates a {@link MssqlTransactionDefinition} retaining all configured options and using the given transaction {@code mark}.
     * Specifies that the transaction is marked in the log. This method updates the transaction name to {@code mark} if no name was set.
     * <p>
     * If {@code WITH MARK} is used, a transaction name must be specified. {@code WITH MARK} allows for restoring a transaction log to a named mark.
     *
     * @param mark describes the mark. A description longer than 128 characters is truncated
     *             to 128 characters before being stored in the {@code msdb.dbo.logmarkhistory} table.
     * @return a new {@link MssqlTransactionDefinition} retaining all configured options and using the given transaction {@code mark}.
     * @throws IllegalArgumentException if {@code mark} is {@code null}.
     */
    MssqlTransactionDefinition mark(String mark);

}
