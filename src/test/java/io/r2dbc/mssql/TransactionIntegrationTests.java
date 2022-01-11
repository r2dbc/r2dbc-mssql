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
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Integration tests for transactional behavior.
 *
 * @author Mark Paluch
 */
class TransactionIntegrationTests extends IntegrationTestSupport {

    @Test
    void savepointsSynchronized() {

        createTable(connection);

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
            .bind(0, 0).bind(1, "Walter").bind(2, "White").add()
            .bind(0, 1).bind(1, "Jesse").bind(2, "Pinkman").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

        connection.createSavepoint("savepoint")
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
            .bind(0, 2).bind(1, "Hank").bind(2, "Schrader").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example")
            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(3)
            .verifyComplete();

        connection.rollbackTransactionToSavepoint("savepoint")
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* in-tx */")
            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(2)
            .verifyComplete();

        connection.commitTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* after-tx */")
            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(2)
            .verifyComplete();
    }

    @Test
    void savepointsConcatWith() {

        createTable(connection);

        connection.beginTransaction()
            .cast(Object.class)
            .concatWith(Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
                .bind(0, 0).bind(1, "Walter").bind(2, "White").execute())
                .flatMap(Result::getRowsUpdated))
            .concatWith(connection.createSavepoint("savepoint"))
            .concatWith(Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
                .bind(0, 2).bind(1, "Hank").bind(2, "Schrader").execute())
                .flatMap(Result::getRowsUpdated))
            .concatWith(connection.rollbackTransactionToSavepoint("savepoint"))
            .concatWith(Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* in-tx */")
                .execute())
                .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class))))
            .concatWith(connection.commitTransaction())
            .concatWith(Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* after-tx */")
                .execute())
                .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class))))
            .as(StepVerifier::create)
            .expectNext(1).as("Affected Rows Count from first INSERT")
            .expectNext(1).as("Affected Rows Count from second INSERT")
            .expectNext(1).as("SELECT COUNT(*) after ROLLBACK TO SAVEPOINT")
            .expectNext(1).as("SELECT COUNT(*) after COMMIT")
            .verifyComplete();
    }

    @Test
    void autoCommitDisabled() {

        createTable(connection);

        connection.setAutoCommit(false)
            .as(StepVerifier::create)
            .verifyComplete();

        connection.createStatement("INSERT INTO r2dbc_example VALUES(0, 'Walter', 'White')").execute()
            .<Object>flatMap(Result::getRowsUpdated)
            .concatWith(connection.rollbackTransaction())
            .as(StepVerifier::create)
            .expectNext(1).as("Affected Rows Count from first INSERT")
            .verifyComplete();

        connectionFactory.create().flatMapMany(c -> c.createStatement("SELECT * FROM r2dbc_example")
            .execute().flatMap(it -> it.map((row, metadata) -> row.get("first_name"))))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void savepointStartsTransaction() {

        createTable(connection);

        connection.createStatement("INSERT INTO r2dbc_example VALUES(1, 'Jesse', 'Pinkman')")
            .execute().flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createSavepoint("s1")
            .thenMany(connection.createStatement("INSERT INTO r2dbc_example VALUES(0, 'Walter', 'White')").execute()
                .<Object>flatMap(Result::getRowsUpdated))
            .concatWith(Mono.fromSupplier(() -> connection.isAutoCommit()))
            .concatWith(connection.rollbackTransaction())
            .as(StepVerifier::create)
            .expectNext(1).as("Affected Rows Count from first INSERT")
            .expectNext(false).as("Auto-commit disabled by createSavepoint")
            .verifyComplete();

        connection.createStatement("INSERT INTO r2dbc_example VALUES(0, 'Walter', 'White')").execute()
            .<Object>flatMap(Result::getRowsUpdated)
            .concatWith(connection.rollbackTransaction())
            .as(StepVerifier::create)
            .expectNext(1).as("Affected Rows Count from second INSERT")
            .verifyComplete();

        connectionFactory.create().flatMapMany(c -> c.createStatement("SELECT * FROM r2dbc_example")
            .execute().flatMap(it -> it.map((row, metadata) -> row.get("first_name"))))
            .as(StepVerifier::create)
            .expectNext("Jesse")
            .verifyComplete();
    }

    @Test
    void commitTransaction() {

        createTable(connection);

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
            .bind(0, 0).bind(1, "Walter").bind(2, "White").add()
            .bind(0, 1).bind(1, "Jesse").bind(2, "Pinkman").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

        connection.commitTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example")
            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(2)
            .verifyComplete();
    }

    @Test
    void rollbackTransaction() {

        createTable(connection);

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
            .bind(0, 0).bind(1, "Walter").bind(2, "White").add()
            .bind(0, 1).bind(1, "Jesse").bind(2, "Pinkman").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

        connection.rollbackTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example")
            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(0)
            .verifyComplete();
    }

    private void createTable(MssqlConnection connection) {

        connection.createStatement("DROP TABLE r2dbc_example").execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE r2dbc_example (" +
                "id int PRIMARY KEY, " +
                "first_name varchar(255), " +
                "last_name varchar(255))")
                .execute().flatMap(MssqlResult::getRowsUpdated).then())
            .as(StepVerifier::create)
            .verifyComplete();
    }
}
