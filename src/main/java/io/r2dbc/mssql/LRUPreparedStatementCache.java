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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static io.r2dbc.mssql.util.Assert.isTrue;
import static io.r2dbc.mssql.util.Assert.requireNonNull;

/**
 * {@link PreparedStatementCache} implementation that maintains a simple "least recently used" cache.
 * By default, this cache has a maximum size of 32.
 *
 * @author Suraj Vijayakumar
 */
class LRUPreparedStatementCache implements PreparedStatementCache {

    private static final int DEFAULT_MAX_SIZE = 32;

    private final Map<String, Integer> handleCache;

    private final Map<String, Object> sqlCache;

    public LRUPreparedStatementCache() {
        this(DEFAULT_MAX_SIZE);
    }

    public LRUPreparedStatementCache(int maxSize) {
        isTrue(maxSize > 0, "Max cache size must be > 0");

        handleCache = new LRUCache<>(maxSize);
        sqlCache = new LRUCache<>(maxSize);
    }

    @Override
    public int getHandle(String sql, Binding binding) {
        requireNonNull(sql, "SQL query must not be null");
        requireNonNull(binding, "Binding must not be null");

        String key = createKey(sql, binding);
        return handleCache.getOrDefault(key, UNPREPARED);
    }

    @Override
    public void putHandle(int handle, String sql, Binding binding) {
        requireNonNull(sql, "SQL query must not be null");
        requireNonNull(binding, "Binding must not be null");

        String key = createKey(sql, binding);
        handleCache.put(key, handle);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getParsedSql(String sql, Function<String, T> parseFunction) {
        requireNonNull(sql, "SQL query must not be null");
        requireNonNull(parseFunction, "Parse function must not be null");

        return (T) sqlCache.computeIfAbsent(sql, parseFunction);
    }

    @Override
    public int size() {
        return handleCache.size();
    }

    private static String createKey(String sql, Binding binding) {
        return sql + "-" + binding.getFormalParameters();
    }

    private static class LRUCache<K, V> extends LinkedHashMap<K, V> {

        private final int maxSize;

        LRUCache(int maxSize) {
            super(16, .75f, true);

            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > maxSize;
        }
    }
}
