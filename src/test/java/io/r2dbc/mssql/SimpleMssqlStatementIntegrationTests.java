/*
 * Copyright 2018 the original author or authors.
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
import java.util.concurrent.atomic.AtomicBoolean;

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

    @Test
    void shouldNotEmitRowsDeletedAfterOpeningKeysetCursor() {

        connection.createStatement("DROP TABLE IF EXISTS cursor_rowstat_deleted").execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).verifyComplete();
        connection.createStatement("CREATE TABLE cursor_rowstat_deleted (id int PRIMARY KEY, name varchar(20) NOT NULL)").execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).verifyComplete();
        connection.createStatement("INSERT INTO cursor_rowstat_deleted SELECT TOP 100 n, CONCAT('name-', 1000 - n) FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_columns) numbers")
            .execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).expectNext(100L).verifyComplete();

        MssqlConnection other = connectionFactory.create().block();
        AtomicBoolean deleted = new AtomicBoolean();

        try {

            // ORDER BY a non-indexed column turns the cursor into a keyset cursor. id = 1 is fetched last, after it was deleted by the other connection.
            connection.createStatement("SELECT id, name FROM cursor_rowstat_deleted ORDER BY name").fetchSize(1).execute()
                .flatMap(it -> it.map((row, metadata) -> {

                    if (deleted.compareAndSet(false, true)) {
                        other.createStatement("DELETE FROM cursor_rowstat_deleted WHERE id = 1").execute().flatMap(Result::getRowsUpdated).subscribe();
                    }

                    assertThat(metadata.getColumnMetadatas()).hasSize(2);
                    return row.get("name", String.class);
                }))
                .collectList()
                .as(StepVerifier::create)
                .assertNext(names -> assertThat(names).hasSize(99).doesNotContainNull().doesNotContain("name-999"))
                .verifyComplete();
        } finally {
            other.close().block();
        }

        connection.createStatement("DROP TABLE cursor_rowstat_deleted").execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).verifyComplete();
    }

    @Test
    void shouldRetainUserColumnNamedRowstat() {

        for (int fetchSize : new int[]{0, 10}) {

            connection.createStatement("SELECT CAST(2 AS int) AS ROWSTAT").fetchSize(fetchSize).execute()
                .flatMap(it -> it.map((row, metadata) -> metadata.getColumnMetadata(0).getName() + "=" + row.get("ROWSTAT", Integer.class)))
                .as(StepVerifier::create)
                .expectNext("ROWSTAT=2")
                .verifyComplete();
        }

        connection.createStatement("DROP TABLE IF EXISTS cursor_rowstat_user").execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).verifyComplete();
        connection.createStatement("CREATE TABLE cursor_rowstat_user (id int PRIMARY KEY, ROWSTAT int)").execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).verifyComplete();
        connection.createStatement("INSERT INTO cursor_rowstat_user VALUES (1, 2), (2, 1)").execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).expectNext(2L).verifyComplete();

        for (int fetchSize : new int[]{0, 1}) {

            connection.createStatement("SELECT id, ROWSTAT FROM cursor_rowstat_user ORDER BY ROWSTAT DESC").fetchSize(fetchSize).execute()
                .flatMap(it -> it.map((row, metadata) -> metadata.getColumnMetadatas().size() + ":" + row.get("id", Integer.class) + "=" + row.get("ROWSTAT", Integer.class)))
                .as(StepVerifier::create)
                .expectNext("2:1=2", "2:2=1")
                .verifyComplete();
        }

        // sp_cursorprepexec and sp_cursorexecute (cached prepared statement)
        for (int i = 0; i < 2; i++) {

            connection.createStatement("SELECT id, ROWSTAT FROM cursor_rowstat_user WHERE id > @id ORDER BY ROWSTAT DESC").bind("@id", 0).fetchSize(1).execute()
                .flatMap(it -> it.map((row, metadata) -> metadata.getColumnMetadatas().size() + ":" + row.get("id", Integer.class) + "=" + row.get("ROWSTAT", Integer.class)))
                .as(StepVerifier::create)
                .expectNext("2:1=2", "2:2=1")
                .verifyComplete();
        }

        connection.createStatement("DROP TABLE cursor_rowstat_user").execute().flatMap(Result::getRowsUpdated).as(StepVerifier::create).verifyComplete();
    }

}
