/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql;

import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.message.token.RpcRequest;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;

import java.util.Locale;

/**
 * Simple SQL statement without SQL parameter (variables) using cursored ({@link RpcRequest}) execution.
 *
 * @author Mark Paluch
 */
final class SimpleCursoredMssqlStatement extends SimpleMssqlStatement {

    public static final int FETCH_SIZE = 128;

    /**
     * Creates a new {@link SimpleCursoredMssqlStatement}.
     *
     * @param client the client to exchange messages with.
     * @param sql    the query to execute.
     */
    SimpleCursoredMssqlStatement(Client client, String sql) {
        super(client, sql);
    }

    @Override
    public Flux<Result> execute() {

        return CursoredQueryMessageFlow.exchange(this.client, CODECS, this.sql, FETCH_SIZE) //
            .windowUntil(it -> false) //
            .map(it -> SimpleMssqlResult.toResult(CODECS, it));
    }

    /**
     * Returns {@literal true} if the query is supported by this {@link MssqlStatement}. Cursored execution is supported for {@literal SELECT} queries.
     *
     * @param sql the query to inspect.
     * @return {@literal true} if the {@code sql} query is supported.
     */
    static boolean supports(String sql) {

        if (sql.isEmpty()) {
            return false;
        }

        char c = sql.charAt(0);

        return (c == 's' || c == 'S') && sql.toLowerCase(Locale.ENGLISH).startsWith("select");
    }
}
