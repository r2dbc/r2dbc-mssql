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
 * Unit tests for {@link NoPreparedStatementCache}
 *
 * @author Suraj Vijayakumar
 */
class NoPreparedStatementCacheTest {

    @Test
    void shouldNotCacheHandle() {
        NoPreparedStatementCache cache = new NoPreparedStatementCache();
        String sql = "test statement";
        Binding binding = new Binding();

        cache.putHandle(100, sql, binding);
        int handle = cache.getHandle(sql, binding);
        int size = cache.size();

        assertThat(handle).isEqualTo(PreparedStatementCache.UNPREPARED);
        assertThat(size).isEqualTo(0);
    }

    @Test
    void shouldNotCacheSql() {
        NoPreparedStatementCache cache = new NoPreparedStatementCache();
        String sql = "raw statement";
        String parsedSql = "parsed statement";

        AtomicInteger counter = new AtomicInteger();
        Function<String, String> incrementCounter = ignore -> {
            counter.getAndIncrement();
            return parsedSql;
        };

        cache.getParsedSql(sql, incrementCounter);
        cache.getParsedSql(sql, incrementCounter);

        assertThat(counter).hasValue(2);
    }
}
