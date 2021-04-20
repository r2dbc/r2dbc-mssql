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
import io.r2dbc.mssql.api.MssqlTransactionDefinition;
import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.client.ConnectionContext;
import io.r2dbc.mssql.client.TransactionStatus;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.mssql.util.Operators;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Duration;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * {@link Connection} to a Microsoft SQL Server.
 *
 * @author Mark Paluch
 * @author Hebert Coelho
 * @see MssqlConnection
 * @see DefaultMssqlResult
 * @see ErrorDetails
 */
public final class MssqlConnection implements Connection {

    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[\\d\\w_]{1,32}");

    private static final Pattern IDENTIFIER128_PATTERN = Pattern.compile("[\\d\\w_]{1,128}");

    private static final Logger logger = Loggers.getLogger(MssqlConnection.class);

    private final Client client;

    private final MssqlConnectionMetadata metadata;

    private final ConnectionContext context;

    private final ConnectionOptions connectionOptions;

    private final Flux<Integer> validationQuery;

    private volatile boolean autoCommit;

    private volatile IsolationLevel isolationLevel;

    private volatile IsolationLevel previousIsolationLevel;

    private volatile boolean resetLockWaitTime = false;

    MssqlConnection(Client client, MssqlConnectionMetadata connectionMetadata, ConnectionOptions connectionOptions) {

        this.client = Assert.requireNonNull(client, "Client must not be null");
        this.metadata = connectionMetadata;
        this.context = client.getContext();
        this.connectionOptions = Assert.requireNonNull(connectionOptions, "ConnectionOptions must not be null");

        TransactionStatus transactionStatus = client.getTransactionStatus();
        this.autoCommit = transactionStatus == TransactionStatus.AUTO_COMMIT;
        this.isolationLevel = IsolationLevel.READ_COMMITTED;
        this.validationQuery = new SimpleMssqlStatement(this.client, connectionOptions, "SELECT 1").fetchSize(0).execute().flatMap(MssqlResult::getRowsUpdated);
    }

    @Override
    public Mono<Void> beginTransaction() {
        return beginTransaction(EmptyTransactionDefinition.INSTANCE);
    }

    @Override
    public Mono<Void> beginTransaction(TransactionDefinition transactionDefinition) {

        return useTransactionStatus(tx -> {

            if (tx == TransactionStatus.STARTED) {
                logger.debug(this.context.getMessage("Skipping begin transaction because status is [{}]"), tx);
                return Mono.empty();
            }

            String name = transactionDefinition.getAttribute(MssqlTransactionDefinition.NAME);
            String mark = transactionDefinition.getAttribute(MssqlTransactionDefinition.MARK);
            IsolationLevel isolationLevel = transactionDefinition.getAttribute(MssqlTransactionDefinition.ISOLATION_LEVEL);
            Duration lockWaitTime = transactionDefinition.getAttribute(MssqlTransactionDefinition.LOCK_WAIT_TIMEOUT);

            StringBuilder builder = new StringBuilder();

            builder.append("BEGIN TRANSACTION");
            if (name != null) {
                String nameToUse = sanitize(name);
                Assert.isTrue(IDENTIFIER_PATTERN.matcher(nameToUse).matches(), "Transaction names must contain only characters and numbers and must not exceed 32 characters");
                builder.append(" ").append(nameToUse);

                if (mark != null) {
                    String markToUse = sanitize(mark);
                    Assert.isTrue(IDENTIFIER128_PATTERN.matcher(markToUse.substring(0, Math.min(128, markToUse.length()))).matches(), "Transaction names must contain only characters and numbers and" +
                        " must not " +
                        "exceed 128 characters");
                    builder.append(' ').append("WITH MARK '").append(markToUse).append("'");
                }
            }
            builder.append(';');

            if (isolationLevel != null) {
                builder.append(renderSetIsolationLevel(isolationLevel)).append(';');
            }

            if (lockWaitTime != null) {
                this.resetLockWaitTime = true;
                builder.append("SET LOCK_TIMEOUT ").append(lockWaitTime.isNegative() ? "-1" : lockWaitTime.toMillis()).append(';');
            }

            logger.debug(this.context.getMessage("Beginning transaction from status [{}]"), tx);

            return exchange(builder.toString()).doOnSuccess(unused -> {

                this.previousIsolationLevel = this.isolationLevel;

                if (isolationLevel != null) {
                    this.isolationLevel = isolationLevel;
                }
            });
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
                logger.debug(this.context.getMessage("Skipping commit transaction because status is [{}]"), tx);
                return Mono.empty();
            }

            logger.debug(this.context.getMessage("Committing transaction with status [{}]"), tx);

            return exchange("IF @@TRANCOUNT > 0 COMMIT TRANSACTION;" + cleanup()).doOnSuccess(v -> {

                if (this.previousIsolationLevel != null) {
                    this.isolationLevel = this.previousIsolationLevel;
                    this.previousIsolationLevel = null;
                }

                this.resetLockWaitTime = false;
            });
        });
    }

    private String cleanup() {

        String cleanupSql = "";
        if (this.previousIsolationLevel != null && this.previousIsolationLevel != this.isolationLevel) {
            cleanupSql = renderSetIsolationLevel(this.previousIsolationLevel) + ";";
        }

        if (this.resetLockWaitTime) {
            cleanupSql += "SET LOCK_TIMEOUT -1;";
        }

        return cleanupSql;
    }

    @Override
    public MssqlBatch createBatch() {
        return new MssqlBatch(this.client, this.connectionOptions);
    }

    @Override
    public Mono<Void> createSavepoint(String name) {

        Assert.requireNonNull(name, "Savepoint name must not be null");

        String nameToUse = sanitize(name);
        Assert.isTrue(IDENTIFIER_PATTERN.matcher(nameToUse).matches(), "Save point names must contain only characters and numbers and must not exceed 32 characters");

        return useTransactionStatus(tx -> {

            logger.debug(this.context.getMessage("Creating savepoint [{}] for transaction with status [{}]"), nameToUse, tx);

            if (this.autoCommit) {
                logger.debug(this.context.getMessage("Setting auto-commit mode to [false]"));
            }

            return exchange(String.format("SET IMPLICIT_TRANSACTIONS ON; IF @@TRANCOUNT = 0 BEGIN BEGIN TRAN IF @@TRANCOUNT = 2 COMMIT TRAN END SAVE TRAN %s;", nameToUse)).doOnSuccess(ignore -> {
                this.autoCommit = false;
            });
        });
    }

    @Override
    public MssqlStatement createStatement(String sql) {

        Assert.requireNonNull(sql, "SQL must not be null");
        logger.debug(this.context.getMessage("Creating statement for SQL: [{}]"), sql);

        if (ParametrizedMssqlStatement.supports(sql)) {
            return new ParametrizedMssqlStatement(this.client, this.connectionOptions, sql);
        }

        return new SimpleMssqlStatement(this.client, this.connectionOptions, sql);
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> rollbackTransaction() {

        return useTransactionStatus(tx -> {

            if (tx != TransactionStatus.STARTED && tx != TransactionStatus.EXPLICIT) {
                logger.debug(this.context.getMessage("Skipping rollback transaction because status is [{}]"), tx);
                return Mono.empty();
            }

            logger.debug(this.context.getMessage("Rolling back transaction with status [{}]"), tx);

            return exchange("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;" + cleanup()).doOnSuccess(v -> {

                if (this.previousIsolationLevel != null) {
                    this.isolationLevel = this.previousIsolationLevel;
                    this.previousIsolationLevel = null;
                }

                this.resetLockWaitTime = false;
            }).doOnSuccess(v -> {

                if (this.previousIsolationLevel != null) {
                    this.isolationLevel = this.previousIsolationLevel;
                    this.previousIsolationLevel = null;
                }

                this.resetLockWaitTime = false;
            });
        });
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {

        Assert.requireNonNull(name, "Savepoint name must not be null");
        String nameToUse = sanitize(name);
        Assert.isTrue(IDENTIFIER_PATTERN.matcher(nameToUse).matches(), "Save point names must contain only characters and numbers and must not exceed 32 characters");

        return useTransactionStatus(tx -> {

            if (tx != TransactionStatus.STARTED) {
                logger.debug(this.context.getMessage("Skipping rollback transaction to savepoint [{}] because status is [{}]"), nameToUse, tx);
                return Mono.empty();
            }

            logger.debug(this.context.getMessage("Rolling back transaction to savepoint [{}] with status [{}]"), nameToUse, tx);

            return exchange(String.format("ROLLBACK TRANSACTION %s", nameToUse));
        });
    }

    public boolean isAutoCommit() {
        return this.autoCommit && this.client.getTransactionStatus() != TransactionStatus.STARTED;
    }

    public Mono<Void> setAutoCommit(boolean autoCommit) {

        return Mono.defer(() -> {

            StringBuilder builder = new StringBuilder();

            logger.debug(this.context.getMessage("Setting auto-commit mode to [{}]"), autoCommit);

            if (this.autoCommit != autoCommit) {

                logger.debug(this.context.getMessage("Committing pending transactions"));
                builder.append("IF @@TRANCOUNT > 0 COMMIT TRAN;");
            }

            builder.append(autoCommit ? "SET IMPLICIT_TRANSACTIONS OFF;" : "SET IMPLICIT_TRANSACTIONS ON;");

            return exchange(builder.toString()).doOnSuccess(ignore -> this.autoCommit = autoCommit);
        });
    }

    @Override
    public MssqlConnectionMetadata getMetadata() {
        return this.metadata;
    }

    public IsolationLevel getTransactionIsolationLevel() {
        return this.isolationLevel;
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        Assert.requireNonNull(isolationLevel, "IsolationLevel must not be null");

        return exchange(renderSetIsolationLevel(isolationLevel)).doOnSuccess(ignore -> this.isolationLevel = isolationLevel);
    }

    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {

        if (depth == ValidationDepth.LOCAL) {
            return Mono.fromSupplier(this.client::isConnected);
        }

        return Mono.create(sink -> {

            if (!this.client.isConnected()) {
                sink.success(false);
                return;
            }

            this.validationQuery.subscribe(new CoreSubscriber<Integer>() {

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Integer.MAX_VALUE);
                }

                @Override
                public void onNext(Integer integer) {

                }

                @Override
                public void onError(Throwable t) {

                    logger.debug("Validation failed", t);
                    sink.success(false);
                }

                @Override
                public void onComplete() {
                    sink.success(true);
                }
            });
        });
    }

    private static String renderSetIsolationLevel(IsolationLevel isolationLevel) {
        return "SET TRANSACTION ISOLATION LEVEL " + isolationLevel.asSql();
    }

    private static String sanitize(String identifier) {
        return identifier.replace('-', '_').replace('.', '_');
    }

    private Mono<Void> exchange(String sql) {

        ExceptionFactory factory = ExceptionFactory.withSql(sql);
        return QueryMessageFlow.exchange(this.client, sql)
            .transform(Operators::discardOnCancel)
            .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)
            .handle(factory::handleErrorResponse)
            .then();
    }

    private Mono<Void> useTransactionStatus(Function<TransactionStatus, Publisher<?>> function) {
        return Flux.defer(() -> function.apply(this.client.getTransactionStatus()))
            .then();
    }

    enum EmptyTransactionDefinition implements TransactionDefinition {
        INSTANCE;

        @Override
        public <T> T getAttribute(Option<T> option) {
            return null;
        }
    }

}
