/*
 * Copyright 2019-2021 the original author or authors.
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

import io.r2dbc.spi.IsolationLevel;

/**
 * SQL Server-specific transaction isolation levels.
 * <p>
 * For more information check:
 * <a href="https://docs.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql?view=sql-server-2017">SQL Server Isolation Levels</a>
 *
 * @author Hebert Coelho
 * @author Mark Paluch
 * @see IsolationLevel
 */
public final class MssqlIsolationLevel {

    private MssqlIsolationLevel() {

    }

    /**
     * The read committed isolation level.
     */
    public static final IsolationLevel READ_COMMITTED = IsolationLevel.READ_COMMITTED;

    /**
     * The read uncommitted isolation level.
     */
    public static final IsolationLevel READ_UNCOMMITTED = IsolationLevel.READ_UNCOMMITTED;

    /**
     * The repeatable read isolation level.
     */
    public static final IsolationLevel REPEATABLE_READ = IsolationLevel.REPEATABLE_READ;

    /**
     * The serializable isolation level.
     */
    public static final IsolationLevel SERIALIZABLE = IsolationLevel.SERIALIZABLE;

    /**
     * The snapshot isolation level.
     */
    public static final IsolationLevel SNAPSHOT = IsolationLevel.valueOf("SNAPSHOT");

    /**
     * Unspecified isolation level.
     *
     * @since 0.9
     */
    public static final IsolationLevel UNSPECIFIED = IsolationLevel.valueOf("UNSPECIFIED");

}
