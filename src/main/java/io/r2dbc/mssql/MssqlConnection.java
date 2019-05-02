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
import io.r2dbc.mssql.client.TransactionStatus;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * {@link Connection} to a Microsoft SQL Server.
 *
 * @author Mark Paluch
 * @author Hebert Coelho
 * @see MssqlConnection
 * @see MssqlResult
 * @see ErrorDetails
 */
public final class MssqlConnection implements Connection {

    private static final Pattern SAVEPOINT_PATTERN = Pattern.compile("[\\d\\w_]{1,32}");

    private static final Logger logger = LoggerFactory.getLogger(MssqlConnection.class);

    private final Client client;

    private final ConnectionOptions connectionOptions;

    MssqlConnection(Client client, ConnectionOptions connectionOptions) {

        this.client = Assert.requireNonNull(client, "Client must not be null");
        this.connectionOptions = Assert.requireNonNull(connectionOptions, "ConnectionOptions must not be null");
    }

    @Override
    public Mono<Void> beginTransaction() {

        return useTransactionStatus(tx -> {

            if (tx == TransactionStatus.STARTED) {
                logger.debug("Skipping begin transaction because status is [{}]", tx);
                return Mono.empty();
            }

            String sql = tx == TransactionStatus.AUTO_COMMIT ? "SET IMPLICIT_TRANSACTIONS OFF; " : "";
            sql += "BEGIN TRANSACTION";

            logger.debug("Beginning transaction from status [{}]", tx);

            return exchange(sql);
        });
    }

    @Override
    public Mono<Void> close() {
        return this.client.close();
    }

    @Override
    public Mono<Void> commitTransaction() {

        return useTransactionStatus(tx -> {

            if (tx != TransactionStatus.STARTED) {
                logger.debug("Skipping commit transaction because status is [{}]", tx);
                return Mono.empty();
            }

            logger.debug("Committing transaction with status [{}]", tx);

            return exchange("IF @@TRANCOUNT > 0 COMMIT TRANSACTION");
        });
    }

    @Override
    public Batch createBatch() {
        return new MssqlBatch(this.client, this.connectionOptions);
    }

    @Override
    public Mono<Void> createSavepoint(String name) {

        Assert.requireNonNull(name, "Savepoint name must not be null");
        Assert.isTrue(SAVEPOINT_PATTERN.matcher(name).matches(), "Save point names must contain only characters and numbers and must not exceed 32 characters");

        return useTransactionStatus(tx -> {

            if (tx != TransactionStatus.STARTED) {
                logger.debug("Skipping savepoint creation because status is [{}]", tx);
                return Mono.empty();
            }

            logger.debug("Creating savepoint for transaction with status [{}]", tx);

            return exchange(String.format("IF @@TRANCOUNT = 0 BEGIN BEGIN TRANSACTION IF @@TRANCOUNT = 2 COMMIT TRANSACTION END SAVE TRANSACTION %s", name));
        });
    }

    @Override
    public MssqlStatement createStatement(String sql) {

        Assert.requireNonNull(sql, "SQL must not be null");
        logger.debug("Creating statement for SQL: [{}]", sql);

        if (ParametrizedMssqlStatement.supports(sql)) {
            return new ParametrizedMssqlStatement(this.client, this.connectionOptions, sql);
        }

        return new SimpleMssqlStatement(this.client, this.connectionOptions, sql);
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        throw new UnsupportedOperationException("Savepoint releasing not supported with SQL Server");
    }

    @Override
    public Mono<Void> rollbackTransaction() {

        return useTransactionStatus(tx -> {

            if (tx != TransactionStatus.STARTED) {
                logger.debug("Skipping rollback transaction because status is [{}]", tx);
                return Mono.empty();
            }

            logger.debug("Rolling back transaction with status [{}]", tx);

            return exchange("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION");
        });
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {

        Assert.requireNonNull(name, "Savepoint name must not be null");
        Assert.isTrue(SAVEPOINT_PATTERN.matcher(name).matches(), "Save point names must contain only characters and numbers and must not exceed 32 characters");

        return useTransactionStatus(tx -> {

            if (tx != TransactionStatus.STARTED) {
                logger.debug("Skipping rollback transaction to savepoint [{}] because status is [{}]", name, tx);
                return Mono.empty();
            }

            logger.debug("Rolling back transaction to savepoint [{}] with status [{}]", name, tx);

            return exchange(String.format("ROLLBACK TRANSACTION %s", name));
        });
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        Assert.requireNonNull(isolationLevel, "IsolationLevel must not be null");

        return exchange("SET TRANSACTION ISOLATION LEVEL " + isolationLevel.asSql());
    }

    private Mono<Void> exchange(String sql) {

        ExceptionFactory factory = ExceptionFactory.withSql(sql);
        return QueryMessageFlow.exchange(this.client, sql).handle(factory::handleErrorResponse).then();
    }

    Client getClient() {
        return this.client;
    }

    private Mono<Void> useTransactionStatus(Function<TransactionStatus, Publisher<?>> function) {
        return Flux.defer(() -> function.apply(this.client.getTransactionStatus()))
            .then();
    }
}
