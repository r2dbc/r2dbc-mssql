/*
 * Copyright 2018-2022 the original author or authors.
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
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link ParametrizedMssqlStatement}.
 *
 * @author Mark Paluch
 */
class ParametrizedMssqlStatementIntegrationTests extends IntegrationTestSupport {

    static {
        Hooks.onOperatorDebug();
    }

    @Test
    void shouldExecuteBatch() {

        connection.createStatement("DROP TABLE r2dbc_example").execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE r2dbc_example (" +
                "id int PRIMARY KEY IDENTITY(1,1), " +
                "first_name varchar(255), " +
                "last_name varchar(255))")
                .execute().flatMap(MssqlResult::getRowsUpdated).then())
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example (first_name, last_name) values (@fn, @ln)")
            .bind("fn", "Walter").bind("ln", "White").add()
            .bind("fn", "Hank").bind("@ln", "Schrader").add()
            .bind("fn", "Skyler").bind("@ln", "White").add()
            .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1, 1, 1)
            .verifyComplete();
    }

    @Test
    void failureShouldNotLockUpConnection() {

        connection.createStatement("DROP TABLE r2dbc_example").execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE r2dbc_example (" +
                "id int NOT NULL, " +
                "first_name varchar(255), " +
                "last_name varchar(255))")
                .execute().flatMap(MssqlResult::getRowsUpdated).then())
            .as(StepVerifier::create)
            .verifyComplete();

        for (int i = 0; i < 10; i++) {

            connection.createStatement("INSERT INTO r2dbc_example (id, first_name) VALUES(@P1, @P2)")
                .bindNull("@P1", Integer.class)
                .bind("@P2", "foo")
                .returnGeneratedValues()
                .execute()
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .verifyError();
        }
    }

    @Test
    void shouldDecodeNull() {

        shouldExecuteBatch();

        Flux.from(connection.createStatement("SELECT null, first_name FROM r2dbc_example")
            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> {
                return Optional.ofNullable(row.get(0));
            }))
            .as(StepVerifier::create)
            .expectNext(Optional.empty(), Optional.empty(), Optional.empty())
            .verifyComplete();
    }

    @Test
    void shouldRunQueryWithLocalVariableDeclarations() {

        Flux.from(connection.createStatement("declare @i int = 1; select @i where @x = 1")
            .bind("x", 1)
            .execute())
            .flatMap(it -> it.map((r, m) -> r.get(0)))
            .as(StepVerifier::create).expectNextCount(1).verifyComplete();
    }

    @Test
    void shouldEmitSingleResultForCursoredExecution() {

        shouldExecuteBatch();

        AtomicInteger resultCounter = new AtomicInteger();
        AtomicInteger rowCounter = new AtomicInteger();

        Flux.from(connection.createStatement("SELECT first_name FROM r2dbc_example")
            .fetchSize(2)
            .execute())
            .flatMap(result -> {

                resultCounter.incrementAndGet();
                return result.map((row, rowMetadata) -> new Object()).doOnNext(it -> rowCounter.incrementAndGet()).then();
            })
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(resultCounter).hasValue(1);
        assertThat(rowCounter).hasValue(3);
    }

    @Test
    void shouldRunStatementWithMultipleResults() {

        AtomicInteger resultCounter = new AtomicInteger();
        AtomicInteger firstUpdateCount = new AtomicInteger();
        AtomicInteger rowCount = new AtomicInteger();

        Flux.from(connection.createStatement("DECLARE @t TABLE(i INT);INSERT INTO @t VALUES (@P1),(2),(3);SELECT * FROM @t;\n")
            .bind("@P1", 1)
            .execute()).flatMap(it -> {

            if (resultCounter.compareAndSet(0, 1)) {
                return it.getRowsUpdated().doOnNext(firstUpdateCount::set).then();
            }

            if (resultCounter.incrementAndGet() == 2) {
                return it.map(((row, rowMetadata) -> {
                    rowCount.incrementAndGet();

                    return new Object();
                })).then();
            }

            throw new IllegalStateException("Unexpected result");
        }).as(StepVerifier::create).verifyComplete();

        assertThat(resultCounter).hasValue(2);
        assertThat(firstUpdateCount).hasValue(3);
        assertThat(rowCount).hasValue(3);
    }

    @Test
    void shouldRunStatementWithMultipleBindingsAndResults() {

        AtomicBoolean firstGurard = new AtomicBoolean();
        AtomicBoolean secondGurard = new AtomicBoolean();
        AtomicBoolean thirdGurard = new AtomicBoolean();
        AtomicBoolean fourthGurard = new AtomicBoolean();

        AtomicInteger firstUpdateCount = new AtomicInteger();
        AtomicInteger secondUpdateCount = new AtomicInteger();
        AtomicInteger rowCount = new AtomicInteger();

        Flux.from(connection.createStatement("DECLARE @t TABLE(i INT);INSERT INTO @t VALUES (@P1),(2),(3);SELECT * FROM @t;\n")
            .bind("@P1", 1).add()
            .bind("@P1", 2)
            .execute()).flatMap(it -> {

            if (firstGurard.compareAndSet(false, true)) {
                return it.getRowsUpdated().doOnNext(firstUpdateCount::set).then();
            }

            if (secondGurard.compareAndSet(false, true)) {
                return it.map(((row, rowMetadata) -> {
                    rowCount.incrementAndGet();

                    return new Object();
                })).then();
            }

            if (thirdGurard.compareAndSet(false, true)) {
                return it.getRowsUpdated().doOnNext(secondUpdateCount::set).then();
            }

            if (fourthGurard.compareAndSet(false, true)) {
                return it.map(((row, rowMetadata) -> {
                    rowCount.incrementAndGet();

                    return new Object();
                })).then();
            }

            throw new IllegalStateException("Unexpected result");
        }).as(StepVerifier::create).verifyComplete();

        assertThat(firstUpdateCount).hasValue(3);
        assertThat(secondUpdateCount).hasValue(3);
        assertThat(rowCount).hasValue(6);
    }

}
