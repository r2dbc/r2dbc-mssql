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
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.message.token.AbstractDoneToken;
import io.r2dbc.mssql.message.token.SqlBatch;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;

import java.util.Objects;

import static io.r2dbc.mssql.util.PredicateUtils.or;

/**
 * Simple SQL statement without SQL parameter (variables) using direct ({@link SqlBatch}) execution.
 *
 * @author Mark Paluch
 */
class SimpleMssqlStatement implements MssqlStatement<SimpleMssqlStatement> {

    static final Codecs CODECS = new DefaultCodecs();

    final Client client;

    final String sql;

    SimpleMssqlStatement(Client client, String sql) {

        Objects.requireNonNull(client, "Client must not be null");
        Objects.requireNonNull(sql, "SQL must not be null");

        Assert.isTrue(sql.trim().length() > 0, "SQL must contain text");

        this.client = client;
        this.sql = sql;
    }

    @Override
    public SimpleMssqlStatement add() {
        return this;
    }

    @Override
    public SimpleMssqlStatement bind(Object identifier, Object value) {
        throw new UnsupportedOperationException(
            String.format("Binding parameters is not supported for the statement [%s]", this.sql));
    }

    @Override
    public SimpleMssqlStatement bind(int index, Object value) {
        throw new UnsupportedOperationException(
            String.format("Binding parameters is not supported for the statement [%s]", this.sql));
    }

    @Override
    public SimpleMssqlStatement bindNull(Object identifier, Class<?> type) {
        throw new UnsupportedOperationException(
            String.format("Binding parameters is not supported for the statement [%s]", this.sql));
    }

    @Override
    public SimpleMssqlStatement bindNull(int index, Class<?> type) {
        throw new UnsupportedOperationException(
            String.format("Binding parameters is not supported for the statement [%s]", this.sql));
    }

    @Override
    public Flux<Result> execute() {

        return QueryMessageFlow.exchange(client, this.sql) //
            .windowUntil(or(AbstractDoneToken.class::isInstance)) //
            .map(it -> SimpleMssqlResult.toResult(CODECS, it));
    }
}
