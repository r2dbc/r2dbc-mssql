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

import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.client.ConnectionContext;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.token.AbstractDoneToken;
import io.r2dbc.mssql.message.token.DoneInProcToken;
import io.r2dbc.mssql.message.token.SqlBatch;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.mssql.util.Operators;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.Locale;
import java.util.function.Predicate;

/**
 * Simple SQL statement without SQL parameter (variables) using direct ({@link SqlBatch}) execution.
 *
 * @author Mark Paluch
 */
final class SimpleMssqlStatement extends MssqlStatementSupport implements MssqlStatement {

    private static final Logger logger = Loggers.getLogger(SimpleMssqlStatement.class);

    private final Client client;

    private final Codecs codecs;

    private final ConnectionContext context;

    private final String sql;

    /**
     * Creates a new {@link SimpleMssqlStatement}.
     *
     * @param client            the client to exchange messages with.
     * @param connectionOptions the connection options.
     * @param sql               the query to execute.
     * @throws IllegalArgumentException when {@link Client}, {@link ConnectionOptions}, or {@code sql} is {@code null}.
     */
    SimpleMssqlStatement(Client client, ConnectionOptions connectionOptions, String sql) {

        super(connectionOptions.prefersCursors(sql) || prefersCursors(sql));

        Assert.requireNonNull(client, "Client must not be null");
        Assert.requireNonNull(connectionOptions, "ConnectionOptions must not be null");
        Assert.requireNonNull(sql, "SQL must not be null");

        Assert.isTrue(sql.trim().length() > 0, "SQL must contain text");

        this.client = client;
        this.context = client.getContext();
        this.codecs = connectionOptions.getCodecs();
        this.sql = sql;
    }

    @Override
    public SimpleMssqlStatement add() {
        return this;
    }

    @Override
    public SimpleMssqlStatement bind(String identifier, Object value) {
        throw new UnsupportedOperationException(
            String.format("Binding parameters is not supported for the statement [%s]", this.sql));
    }

    @Override
    public SimpleMssqlStatement bind(int index, Object value) {
        throw new UnsupportedOperationException(
            String.format("Binding parameters is not supported for the statement [%s]", this.sql));
    }

    @Override
    public SimpleMssqlStatement bindNull(String identifier, Class<?> type) {
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

        int effectiveFetchSize = getEffectiveFetchSize();

        return Flux.defer(() -> {

            boolean useGeneratedKeysClause = GeneratedValues.shouldExpectGeneratedKeys(this.getGeneratedColumns());
            String sql = useGeneratedKeysClause ? GeneratedValues.augmentQuery(this.sql, getGeneratedColumns()) : this.sql;

            Flux<Message> exchange;

            if (effectiveFetchSize > 0) {

                if (logger.isDebugEnabled()) {
                    logger.debug(this.context.getMessage("Start cursored exchange for {} with fetch size {}"), sql, effectiveFetchSize);
                }

                exchange = RpcQueryMessageFlow.exchange(this.client, this.codecs, this.sql, effectiveFetchSize);

                return createResultStream(useGeneratedKeysClause, exchange, DoneInProcToken.class::isInstance);
            } else {

                if (logger.isDebugEnabled()) {
                    logger.debug(this.context.getMessage("Start direct exchange for {}"), sql);
                }

                exchange = QueryMessageFlow.exchange(this.client, sql).transform(Operators::discardOnCancel).doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release);

                return createResultStream(useGeneratedKeysClause, exchange, AbstractDoneToken.class::isInstance);
            }
        });
    }

    private Publisher<MssqlResult> createResultStream(boolean useGeneratedKeysClause, Flux<Message> exchange, Predicate<Message> windowUntil) {
        if (useGeneratedKeysClause) {
            exchange = exchange.transform(GeneratedValues::reduceToSingleCountDoneToken);
        }

        return exchange.windowUntil(windowUntil) //
            .map(it -> MssqlSegmentResult.toResult(this.sql, this.context, this.codecs, it, false));
    }

    @Override
    public SimpleMssqlStatement returnGeneratedValues(String... columns) {

        super.returnGeneratedValues(columns);
        return this;
    }

    @Override
    public SimpleMssqlStatement fetchSize(int fetchSize) {

        super.fetchSize(fetchSize);
        return this;
    }

    /**
     * Returns {@code true} if the query is supported by this {@link MssqlStatement}. Cursored execution is supported for {@literal SELECT} queries.
     *
     * @param sql the query to inspect.
     * @return {@code true} if the {@code sql} query is supported.
     */
    static boolean prefersCursors(String sql) {

        if (sql.isEmpty()) {
            return false;
        }

        char c = sql.charAt(0);

        return (c == 's' || c == 'S') && sql.toLowerCase(Locale.ENGLISH).startsWith("select");
    }

}
