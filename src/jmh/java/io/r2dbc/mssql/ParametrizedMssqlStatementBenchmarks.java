/*
 * Copyright 2019-2022 the original author or authors.
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

import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.codec.DefaultCodecs;
import org.junit.platform.commons.annotation.Testable;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;

import java.util.function.Function;

/**
 * Benchmarks for {@link ParametrizedMssqlStatement}, specifically query parsing.
 *
 * @author Mark Paluch
 */
@Testable
public class ParametrizedMssqlStatementBenchmarks extends BenchmarkSettings {

    private static final ConnectionOptions cached = new ConnectionOptions(s -> false, new DefaultCodecs(), new IndefinitePreparedStatementCache(), true);

    private static final ConnectionOptions uncached = new ConnectionOptions(s -> false, new DefaultCodecs(), new PreparedStatementCache() {

        @Override
        public int getHandle(Object connectionKey, String sql, Binding binding) {
            return 0;
        }

        @Override
        public void putHandle(Object connectionKey, int handle, String sql, Binding binding) {
        }

        @Override
        public <T> T getParsedSql(String sql, Function<String, T> parseFunction) {
            return parseFunction.apply(sql);
        }

        @Override
        public int size() {
            return 0;
        }
    }, true);

    private static final Client client = Mockito.mock(Client.class);

    @Benchmark
    public Object parseSqlCached1Param() {
        return new ParametrizedMssqlStatement(client, cached, "SELECT * from FOO where firstname = @firstname");
    }

    @Benchmark
    public Object parseSqlCached5Param() {
        return new ParametrizedMssqlStatement(client, cached, "SELECT * from FOO where firstname = @firstname and firstname = @firstname and p2 = @p2 and p3 = @p3 and p4 = @p4 and p5 = @p5");
    }

    @Benchmark
    public Object parseSqlNonCached1Param() {
        return new ParametrizedMssqlStatement(client, uncached, "SELECT * from FOO where firstname = @firstname");
    }

    @Benchmark
    public Object parseSqlNonCached5Param() {
        return new ParametrizedMssqlStatement(client, uncached, "SELECT * from FOO where firstname = @firstname and firstname = @firstname and p2 = @p2 and p3 = @p3 and p4 = @p4 and p5 = @p5");
    }
}
