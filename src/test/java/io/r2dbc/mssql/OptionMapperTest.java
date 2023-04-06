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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link OptionMapper}
 *
 * @author Suraj Vijayakumar
 */
class OptionMapperTest {

    @Test
    void shouldConvertNegativeIntegerToIndefinitePreparedStatementCache() {
        PreparedStatementCache cache = OptionMapper.toPreparedStatementCache(-5);
        assertThat(cache).isInstanceOf(IndefinitePreparedStatementCache.class);
    }

    @Test
    void shouldConvertZeroToNoPreparedStatementCache() {
        PreparedStatementCache cache = OptionMapper.toPreparedStatementCache(0);
        assertThat(cache).isInstanceOf(NoPreparedStatementCache.class);
    }

    @Test
    void shouldConvertOtherIntegerToLRUPreparedStatementCache() {
        PreparedStatementCache cache = OptionMapper.toPreparedStatementCache(10);
        assertThat(cache).isInstanceOf(LRUPreparedStatementCache.class);
    }

    @Test
    void shouldConvertStringNumberToPreparedStatementCache() {
        PreparedStatementCache cache = OptionMapper.toPreparedStatementCache("0");
        assertThat(cache).isInstanceOf(NoPreparedStatementCache.class);
    }

    @Test
    void shouldConvertStringClassToPreparedStatementCache() {
        PreparedStatementCache cache = OptionMapper.toPreparedStatementCache("io.r2dbc.mssql.LRUPreparedStatementCache");
        assertThat(cache).isInstanceOf(LRUPreparedStatementCache.class);
    }
}
