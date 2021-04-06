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

package io.r2dbc.mssql;

import java.util.function.Function;

/**
 * Cache for prepared statements.
 *
 * @author Mark Paluch
 */
interface PreparedStatementCache {

    /**
     * Marker for no prepared statement found/no prepared statement.
     */
    int UNPREPARED = 0;

    /**
     * Returns a prepared statement handle for the given {@code sql} query and the {@link Binding}.
     *
     * @param sql     the SQL query.
     * @param binding bound parameters. Parameter types impact the prepared query.
     * @return the prepared statement handle. {@value 0} has a specific meaning as it indicates that no cached SQL statement was found.
     * @see #UNPREPARED
     */
    int getHandle(String sql, Binding binding);

    /**
     * Returns a prepared statement {@code handle} for the given {@code sql} query and the {@link Binding}.
     *
     * @param handle  the prepared statement handle.
     * @param sql     the SQL query.
     * @param binding bound parameters. Parameter types impact the prepared query.
     */
    void putHandle(int handle, String sql, Binding binding);

    /**
     * Returns the parsed and potentially cached representation of the {@code sql} statement.
     *
     * @param sql           query to parse.
     * @param parseFunction parse function.
     * @param <T>
     * @return the parsed SQL representation.
     */
    <T> T getParsedSql(String sql, Function<String, T> parseFunction);

    /**
     * Returns the number of cached prepared statement handles in this cache.
     *
     * @return the number of prepared statement handles in this cache.
     */
    int size();

}
