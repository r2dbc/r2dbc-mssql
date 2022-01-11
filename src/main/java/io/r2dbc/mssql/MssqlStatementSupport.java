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
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;

import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Base class for {@link Statement} implementations.
 * <p>This class considers {@link #returnGeneratedValues(String...)} and {@link #fetchSize(int)} and cursor/direct execution preferences.
 * <p>Cursor/direct execution preference is considered as initial hint. A statement can be forced to be executed directly by setting {@link #fetchSize(int)} to zero. Alternatively, cursored
 * execution can be forced by setting {@link #fetchSize(int)} to a non-zero value.
 *
 * @author Mark Paluch
 */
abstract class MssqlStatementSupport implements MssqlStatement {

    static final int FETCH_SIZE = 128;

    static final int FETCH_UNCONFIGURED = -1;

    private final boolean preferCursoredExecution;

    @Nullable
    private String[] generatedColumns;

    private int fetchSize = FETCH_UNCONFIGURED;

    MssqlStatementSupport(boolean preferCursoredExecution) {
        this.preferCursoredExecution = preferCursoredExecution;
    }

    /**
     * Returns the effective fetch size.
     * <p>
     * A fetch size of zero indicates direct execution.
     *
     * @return the effective fetch size. Can be zero. Defaults to {@link #FETCH_SIZE} if cursored execution is preferred.
     */
    int getEffectiveFetchSize() {

        if (this.preferCursoredExecution) {
            return this.fetchSize == FETCH_UNCONFIGURED ? FETCH_SIZE : this.fetchSize;
        }

        return this.fetchSize == FETCH_UNCONFIGURED ? 0 : this.fetchSize;
    }

    @Nullable
    String[] getGeneratedColumns() {
        return this.generatedColumns;
    }

    @Override
    public MssqlStatementSupport returnGeneratedValues(String... columns) {

        Assert.requireNonNull(columns, "columns must not be null");

        this.generatedColumns = columns;
        return this;
    }

    @Override
    public MssqlStatementSupport fetchSize(int fetchSize) {

        Assert.isTrue(fetchSize >= 0, "Fetch size must be greater or equal to zero");

        this.fetchSize = fetchSize;
        return this;
    }

    Flux<Message> potentiallyAttachTimeout(Flux<Message> exchange, ConnectionOptions connectionOptions, Client client, String sql) {

        Duration statementTimeout = connectionOptions.getStatementTimeout();

        if (statementTimeout.isZero()) {
            return exchange;
        }

        Mono<Long> timeout = Mono.delay(statementTimeout, Schedulers.parallel()).onErrorReturn(0L);
        return exchange.timeout(timeout).onErrorResume(TimeoutException.class, e -> client.attention().then(Mono.error(new ExceptionFactory.MssqlStatementTimeoutException(String.format("Statement " +
            "did not yield a result within %dms", statementTimeout.toMillis()),
            sql))));
    }

}
