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
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link Statement#returnGeneratedValues(String...)}.
 *
 * @author Mark Paluch
 */
class ReturnGeneratedValuesIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void setUp() {
        createTable(connection);
    }

    @Test
    void simpleStatementShouldReturnNumberOfInsertedRows() {

        Flux<MssqlResult> result = connection.createStatement("INSERT INTO generated_values (Name) VALUES ('Timmy'), ('Johnny')")  //
            .returnGeneratedValues() //
            .execute();

        verifyRowsUpdated(result);
    }

    @Test
    void simpleStatementShouldReturnGeneratedValues() {

        Flux<MssqlResult> result = connection.createStatement("INSERT INTO generated_values (Name) VALUES ('Timmy'), ('Johnny')") //
            .returnGeneratedValues("id") //
            .execute();

        verifyGeneratedValues(result);
    }

    @Test
    void preparedStatementShouldReturnNumberOfInsertedRows() {

        Flux<MssqlResult> result = connection.createStatement("INSERT INTO generated_values (Name) VALUES (@P0), (@P1)")  //
            .bind("P0", "Timmy").bind("P1", "Johnny")
            .returnGeneratedValues() //
            .execute();

        verifyRowsUpdated(result);
    }

    @Test
    void preparedStatementShouldReturnGeneratedValues() {

        Flux<MssqlResult> result = connection.createStatement("INSERT INTO generated_values (Name) VALUES (@P0), (@P1)") //
            .bind("P0", "Timmy").bind("P1", "Johnny")
            .returnGeneratedValues("id") //
            .execute();

        verifyGeneratedValues(result);
    }

    private static void verifyRowsUpdated(Flux<MssqlResult> result) {

        AtomicInteger resultCounter = new AtomicInteger();

        result.flatMap(it -> {
                resultCounter.incrementAndGet();
                return it.getRowsUpdated();
            }).as(StepVerifier::create)
            .expectNext(2L)
            .verifyComplete();

        assertThat(resultCounter).hasValue(1);
    }

    private static void verifyGeneratedValues(Flux<MssqlResult> result) {

        result.flatMap(it -> it.map((row, rowMetadata) -> row.get("id"))) //
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    private void createTable(MssqlConnection connection) {

        connection.createStatement("DROP TABLE generated_values").execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE generated_values (" +
                "id int IDENTITY PRIMARY KEY, " +
                "name varchar(255))")
                .execute().flatMap(MssqlResult::getRowsUpdated).then())
            .as(StepVerifier::create)
            .verifyComplete();
    }
}
