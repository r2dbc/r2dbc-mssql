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
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link SimpleMssqlStatement}.
 *
 * @author Mark Paluch
 */
class SimpleMssqlStatementIntegrationTests extends IntegrationTestSupport {

    @AfterEach
    void resetStatementTimeout() {
        // the connection is shared across the tests of this class
        connection.setStatementTimeout(Duration.ZERO).as(StepVerifier::create).verifyComplete();
    }

    @Test
    void shouldTimeoutSqlBatch() {

        connection.setStatementTimeout(Duration.ofMillis(100)).as(StepVerifier::create).verifyComplete();

        connection.createStatement("WAITFOR DELAY '10:00'").fetchSize(0).execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).verifyError(R2dbcTimeoutException.class);
        connection.createStatement("SELECT 1").execute().flatMap(it -> it.map(row -> row.get(0))).as(StepVerifier::create).expectNext(1).verifyComplete();
    }

    @Test
    void shouldTimeoutCursored() {

        connection.setStatementTimeout(Duration.ofMillis(100)).as(StepVerifier::create).verifyComplete();

        connection.createStatement("WAITFOR DELAY '10:00'").fetchSize(100).execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).verifyError(R2dbcTimeoutException.class);
        connection.createStatement("SELECT 1").execute().flatMap(it -> it.map(row -> row.get(0))).as(StepVerifier::create).expectNext(1).verifyComplete();
    }

    @Test
    void shouldReleaseConversationOnErrorWhileFetching() {

        connection.createStatement("DROP TABLE IF EXISTS cursor_error").execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).verifyComplete();
        connection.createStatement("CREATE TABLE cursor_error (id int)").execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).verifyComplete();
        connection.createStatement("INSERT INTO cursor_error VALUES (1), (2), (3), (4), (5), (6)").execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).expectNext(6L).verifyComplete();

        // fetchSize 2 leaves the cursor in FETCHING with a non-zero cursor id by the time row 6 fails
        connection.createStatement("SELECT CASE WHEN id = 6 THEN 1/0 ELSE id END FROM cursor_error ORDER BY id").fetchSize(2).execute()
                .flatMap(it -> it.map((row, metadata) -> row.get(0))).as(StepVerifier::create).expectNextCount(5)
                .expectErrorSatisfies(error -> assertThat(error).isNotInstanceOf(R2dbcTimeoutException.class).hasMessageContaining("Divide by zero"))
                .verify(Duration.ofSeconds(10));

        // the request/response window must have been released, otherwise the next exchange queues forever
        connection.createStatement("SELECT 1").execute().flatMap(it -> it.map(row -> row.get(0))).as(StepVerifier::create).expectNext(1).expectComplete().verify(Duration.ofSeconds(5));

        connection.createStatement("DROP TABLE cursor_error").execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).verifyComplete();
    }

}
