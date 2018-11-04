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
import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An implementation of {@link Batch} for executing a collection of statements in a batch against a Microsoft SQL Server database.
 *
 * @author Mark Paluch
 */
public final class MssqlBatch implements Batch<MssqlBatch> {

    private final Client client;

    private final Codecs codecs;

    private final List<String> statements = new ArrayList<>();

    /**
     * @param client
     * @param codecs
     */
    MssqlBatch(Client client, Codecs codecs) {

        this.client = Objects.requireNonNull(client, "Client must not be null");
        this.codecs = Objects.requireNonNull(codecs, "Codecs must not be null");
    }

    @Override
    public MssqlBatch add(String sql) {

        Objects.requireNonNull(sql, "SQL must not be null");

        this.statements.add(sql);
        return this;
    }

    @Override
    public Flux<MssqlResult> execute() {
        return new SimpleMssqlStatement(this.client, codecs, String.join("; ", this.statements))
            .execute();
    }

    @Override
    public String toString() {
        return "PostgresqlBatch{" +
            "client=" + this.client +
            ", codecs=" + this.codecs +
            ", statements=" + this.statements +
            '}';
    }
}
