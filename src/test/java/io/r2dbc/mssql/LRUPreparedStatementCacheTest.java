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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LRUPreparedStatementCache}
 *
 * @author Suraj Vijayakumar
 */
class LRUPreparedStatementCacheTest {

    @Test
    void shouldCacheHandle() {
        LRUPreparedStatementCache cache = new LRUPreparedStatementCache(10);
        String sql = "test statement";
        Binding binding = new Binding();

        cache.putHandle(100, sql, binding);
        int handle = cache.getHandle(sql, binding);
        int size = cache.size();

        assertThat(handle).isEqualTo(100);
        assertThat(size).isEqualTo(1);
    }

    @Test
    void shouldCacheSql() {
        LRUPreparedStatementCache cache = new LRUPreparedStatementCache(10);
        String sql = "raw statement";
        String parsedSql = "parsed statement";

        AtomicInteger counter = new AtomicInteger();
        Function<String, String> incrementCounter = ignore -> {
            counter.getAndIncrement();
            return parsedSql;
        };

        cache.getParsedSql(sql, incrementCounter);
        cache.getParsedSql(sql, incrementCounter);

        assertThat(counter).hasValue(1);
    }

    @Test
    void shouldEvictLeastRecentlyUsedHandle() {
        LRUPreparedStatementCache cache = new LRUPreparedStatementCache(2);
        String sql1 = "test statement1";
        String sql2 = "test statement2";
        String sql3 = "test statement3";
        Binding binding = new Binding();

        cache.putHandle(100, sql1, binding);
        cache.putHandle(101, sql2, binding);

        cache.getHandle(sql1, binding);

        cache.putHandle(102, sql3, binding);

        int handle1 = cache.getHandle(sql1, binding);
        int handle2 = cache.getHandle(sql2, binding);
        int handle3 = cache.getHandle(sql3, binding);
        int size = cache.size();

        assertThat(handle1).isEqualTo(100);
        assertThat(handle2).isEqualTo(PreparedStatementCache.UNPREPARED);
        assertThat(handle3).isEqualTo(102);
        assertThat(size).isEqualTo(2);
    }
}
