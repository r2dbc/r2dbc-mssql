/*
 * Copyright 2018-2019 the original author or authors.
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
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.token.AbstractDoneToken;
import io.r2dbc.mssql.message.token.SqlBatch;
import io.r2dbc.mssql.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

/**
 * Simple SQL statement without SQL parameter (variables) using direct ({@link SqlBatch}) execution.
 *
 * @author Mark Paluch
 */
class SimpleMssqlStatement implements MssqlStatement {

    private static final Logger logger = LoggerFactory.getLogger(SimpleMssqlStatement.class);

    final Client client;

    final Codecs codecs;

    final String sql;

    String[] generatedColumns;

    /**
     * Creates a new {@link SimpleMssqlStatement}.
     *
     * @param client            the client to exchange messages with.
     * @param connectionOptions the connection options.
     * @param sql               the query to execute.
     * @throws IllegalArgumentException when {@link Client}, {@link ConnectionOptions}, or {@code sql} is {@code null}.
     */
    SimpleMssqlStatement(Client client, ConnectionOptions connectionOptions, String sql) {

        Assert.requireNonNull(client, "Client must not be null");
        Assert.requireNonNull(connectionOptions, "ConnectionOptions must not be null");
        Assert.requireNonNull(sql, "SQL must not be null");

        Assert.isTrue(sql.trim().length() > 0, "SQL must contain text");

        this.client = client;
        this.codecs = connectionOptions.getCodecs();
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
    public Flux<MssqlResult> execute() {

        boolean useGeneratedKeysClause = GeneratedValues.shouldExpectGeneratedKeys(this.generatedColumns);
        String sql = useGeneratedKeysClause ? GeneratedValues.augmentQuery(this.sql, generatedColumns) : this.sql;

        if (logger.isDebugEnabled()) {
            logger.debug("Start exchange for {}", sql);
        }

        Flux<Message> exchange = QueryMessageFlow.exchange(this.client, sql);

        if (useGeneratedKeysClause) {
            exchange = exchange.transform(GeneratedValues::reduceToSingleCountDoneToken);
        }

        return exchange.windowUntil(AbstractDoneToken.class::isInstance) //
            .map(it -> MssqlResult.toResult(this.codecs, it));
    }

    @Override
    public SimpleMssqlStatement returnGeneratedValues(String... columns) {

        Assert.requireNonNull(columns, "columns must not be null");

        this.generatedColumns = columns;
        return this;
    }
}
