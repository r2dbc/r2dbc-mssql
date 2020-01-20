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

package io.r2dbc.mssql;

import io.r2dbc.mssql.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Cache that stores prepared statement handles eternally.
 *
 * @author Mark Paluch
 */
class IndefinitePreparedStatementCache implements PreparedStatementCache {

    private final Map<String, Integer> preparedStatements = new ConcurrentHashMap<>();

    private final Map<String, Object> parsedSql = new ConcurrentHashMap<>();

    @Override
    public int getHandle(String sql, Binding binding) {

        Assert.requireNonNull(sql, "SQL query must not be null");
        Assert.requireNonNull(binding, "Binding query must not be null");

        return this.preparedStatements.getOrDefault(createKey(sql, binding), UNPREPARED);
    }

    @Override
    public void putHandle(int handle, String sql, Binding binding) {

        Assert.requireNonNull(sql, "SQL query must not be null");
        Assert.requireNonNull(binding, "Binding query must not be null");

        this.preparedStatements.put(createKey(sql, binding), handle);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getParsedSql(String sql, Function<String, T> parseFunction) {
        return (T) this.parsedSql.computeIfAbsent(sql, parseFunction);
    }

    @Override
    public int size() {
        return this.preparedStatements.size();
    }

    private static String createKey(String sql, Binding binding) {
        return sql + "-" + binding.getFormalParameters();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [preparedStatements=").append(this.preparedStatements);
        sb.append(", parsedSql=").append(this.parsedSql);
        sb.append(']');
        return sb.toString();
    }
}
