/*
 * Copyright 2018-2020 the original author or authors.
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
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link Batch} for executing a collection of statements in a batch against a Microsoft SQL Server database.
 *
 * @author Mark Paluch
 */
public final class MssqlBatch implements Batch {

    private final Client client;

    private final ConnectionOptions connectionOptions;

    private final List<String> statements = new ArrayList<>();

    MssqlBatch(Client client, ConnectionOptions connectionOptions) {

        this.client = Assert.requireNonNull(client, "Client must not be null");
        this.connectionOptions = Assert.requireNonNull(connectionOptions, "ConnectionOptions must not be null");
    }

    @Override
    public MssqlBatch add(String sql) {

        Assert.requireNonNull(sql, "SQL must not be null");

        this.statements.add(sql);
        return this;
    }

    @Override
    public Flux<MssqlResult> execute() {
        return new SimpleMssqlStatement(this.client, this.connectionOptions, String.join("; ", this.statements))
            .execute();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [client=").append(this.client);
        sb.append(", connectionOptions=").append(this.connectionOptions);
        sb.append(", statements=").append(this.statements);
        sb.append(']');
        return sb.toString();
    }

}
