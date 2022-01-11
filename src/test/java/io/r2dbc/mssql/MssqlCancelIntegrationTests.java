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

import io.r2dbc.mssql.util.IntegrationTestSupport;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Integration tests for {@link Subscription subscription cancellation} {@link MssqlConnection} and {@link MssqlStatement}.
 *
 * @author Mark Paluch
 */
class MssqlCancelIntegrationTests extends IntegrationTestSupport {

    static boolean initialized = false;

    @BeforeEach
    void setUp() {

        if (!initialized) {

            createTable(connection, "r2dbc_example");
            createTable(connection, "r2dbc_empty");

            for (int i = 0; i < 100; i++) {
                insertRecord(connection, i);
            }

            initialized = true;
        }
    }

    @Test
    void shouldCancelUnparametrizedBatch() {

        connection.createStatement("SELECT * FROM r2dbc_example")
            .fetchSize(0)
            .execute()
            .concatMap(it -> it.map((row, metadata) -> row.get("id", Integer.class)))
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(1)
            .expectNextCount(1)
            .thenCancel()
            .verify();

        connection.createStatement("SELECT * FROM r2dbc_empty")
            .execute()
            .flatMap(it -> it.map((row, metadata) -> row.get("id")))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldCancelUnparametrizedCursoredBatch() {

        connection.createStatement("SELECT * FROM r2dbc_example")
            .fetchSize(10)
            .execute()
            .concatMap(it -> it.map((row, metadata) -> row.get("id", Integer.class)))
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(1)
            .expectNextCount(1)
            .thenCancel()
            .verify();

        connection.createStatement("SELECT * FROM r2dbc_empty")
            .execute()
            .flatMap(it -> it.map((row, metadata) -> row.get("id")))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldCancelParametrizedBatch() {

        connection.createStatement("SELECT * FROM r2dbc_example where id != @P1")
            .bind("@P1", -1)
            .fetchSize(0)
            .execute()
            .concatMap(it -> it.map((row, metadata) -> row.get("id", Integer.class)))
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(1)
            .expectNextCount(1)
            .thenCancel()
            .verify();

        connection.createStatement("SELECT * FROM r2dbc_empty")
            .execute()
            .flatMap(it -> it.map((row, metadata) -> row.get("id")))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldCancelParametrizedCursoredBatch() {

        connection.createStatement("SELECT * FROM r2dbc_example where id != @P1")
            .bind("@P1", -1)
            .fetchSize(10)
            .execute()
            .concatMap(it -> it.map((row, metadata) -> row.get("id", Integer.class)))
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(1)
            .expectNextCount(1)
            .thenCancel()
            .verify();

        connection.createStatement("SELECT * FROM r2dbc_empty")
            .execute()
            .flatMap(it -> it.map((row, metadata) -> row.get("id")))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    private void createTable(MssqlConnection connection, String table) {

        connection.createStatement("DROP TABLE " + table).execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE " + table + " (" +
                "id int PRIMARY KEY, " +
                "first_name varchar(255), " +
                "last_name varchar(255))")
                .execute().flatMap(MssqlResult::getRowsUpdated).then())
            .as(StepVerifier::create)
            .verifyComplete();
    }

    private void insertRecord(MssqlConnection connection, int id) {

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@id, @firstname, @lastname)")
            .bind("id", id)
            .bind("firstname", "Walter")
            .bind("lastname", "White")
            .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
    }
}
