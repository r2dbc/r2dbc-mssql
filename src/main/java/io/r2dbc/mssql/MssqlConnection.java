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
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * {@link Connection} to a Microsoft SQL server.
 *
 * @author Mark Paluch
 */
public final class MssqlConnection implements Connection {

    private static final Pattern SAVEPOINT_PATTERN = Pattern.compile("[\\d\\w_]{1,32}");
    
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    MssqlConnection(Client client) {
        this.client = client;
    }

    private final Client client;

    private volatile boolean autoCommit = true;

    @Override
    public Mono<Void> beginTransaction() {

        return QueryMessageFlow.exchange(client, "SET IMPLICIT_TRANSACTIONS OFF; BEGIN TRANSACTION").handle(MssqlException::handleErrorResponse).then();
    }

    @Override
    public Mono<Void> close() {
        return this.client.close();
    }

    @Override
    public Mono<Void> commitTransaction() {
        return QueryMessageFlow.exchange(client, "IF @@TRANCOUNT > 0 COMMIT TRANSACTION").handle(MssqlException::handleErrorResponse).then();
    }

    @Override
    public Batch<?> createBatch() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Mono<Void> createSavepoint(String name) {

        Objects.requireNonNull(name, "Savepoint name must not be null");
        Assert.isTrue(SAVEPOINT_PATTERN.matcher(name).matches(), "Save point names must contain only characters and numbers and must not exceed 32 characters");

        return QueryMessageFlow.exchange(client, String.format("IF @@TRANCOUNT = 0 BEGIN BEGIN TRANSACTION IF @@TRANCOUNT = 2 COMMIT TRANSACTION END SAVE TRANSACTION [%s]", name)) //
            .handle(MssqlException::handleErrorResponse).then();
    }

    @Override
    public MssqlStatement<?> createStatement(String sql) {
        return new SimpleMssqlStatement(this.client, sql);
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        throw new UnsupportedOperationException("Savepoint releasing not supported with SQL Server");
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return QueryMessageFlow.exchange(client, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION").handle(MssqlException::handleErrorResponse).then();
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {

        Objects.requireNonNull(name, "Savepoint name must not be null");
        Assert.isTrue(SAVEPOINT_PATTERN.matcher(name).matches(), "Save point names must contain only characters and numbers and must not exceed 32 characters");

        return QueryMessageFlow.exchange(client, String.format("ROLLBACK TRANSACTION [%s]", name)) //
            .handle(MssqlException::handleErrorResponse).then();
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {

        Objects.requireNonNull(isolationLevel, "IsolationLevel must not be null");

        return QueryMessageFlow.exchange(client, "SET TRANSACTION ISOLATION LEVEL " + getIsolationLevelSql(isolationLevel)).handle(MssqlException::handleErrorResponse).then();
    }

    private static String getIsolationLevelSql(IsolationLevel isolationLevel) {
        switch (isolationLevel) {
            case READ_UNCOMMITTED: {
                return "READ UNCOMMITTED";
            }
            case READ_COMMITTED: {
                return "READ COMMITTED";
            }
            case REPEATABLE_READ: {
                return "REPEATABLE READ";
            }
            case SERIALIZABLE: {
                return "SERIALIZABLE";
            }
            // TODO: add snapshot isolation level.
           /* case SNAPSHOT: {
                return "snapshot";
                break;
            }*/

        }

        throw new IllegalArgumentException("Isolation level " + isolationLevel + " not supported");
    }
}
