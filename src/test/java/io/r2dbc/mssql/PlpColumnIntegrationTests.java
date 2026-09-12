/*
 * Copyright 2026 the original author or authors.
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for rows containing PLP ({@code MAX}) columns followed by further columns.
 */
class PlpColumnIntegrationTests extends IntegrationTestSupport {

    static final int ROWS = 500;

    @BeforeEach
    void setUp() {

        connection.createStatement("DROP TABLE plp_test").execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE plp_test (first NVARCHAR(MAX) NULL, second NVARCHAR(MAX) NULL, id INT NULL, created DATETIME2 NULL)")
                .execute().flatMap(MssqlResult::getRowsUpdated))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldReadMultiplePlpColumnsSpanningPackets() {

        connection.createStatement("INSERT INTO plp_test (first, second, id) SELECT TOP " + ROWS + " REPLICATE(N'a', 1000), REPLICATE(N'b', 1500), 1 FROM sys.all_objects")
            .execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext((long) ROWS)
            .verifyComplete();

        connection.createStatement("SELECT first, second, id FROM plp_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("first", String.class).length() + row.get("second", String.class).length()))
            .as(StepVerifier::create)
            .recordWith(ArrayList::new)
            .expectNextCount(ROWS)
            .consumeRecordedWith(lengths -> assertThat(lengths).containsOnly(2500))
            .expectComplete()
            .verify(Duration.ofSeconds(30));
    }

    @Test
    void shouldReadPlpFollowedByIntSpanningPackets() {

        connection.createStatement("INSERT INTO plp_test (first, second, id) SELECT TOP " + ROWS + " REPLICATE(N'a', 1000), NULL, 42 FROM sys.all_objects")
            .execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext((long) ROWS)
            .verifyComplete();

        connection.createStatement("SELECT first, id FROM plp_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("first", String.class).length() + row.get("id", Integer.class)))
            .as(StepVerifier::create)
            .recordWith(ArrayList::new)
            .expectNextCount(ROWS)
            .consumeRecordedWith(values -> assertThat(values).containsOnly(1042))
            .expectComplete()
            .verify(Duration.ofSeconds(30));
    }


    @Test
    void shouldReadPlpNullFollowedByDatetimeUsingCursor() {

        connection.createStatement("INSERT INTO plp_test (first, id, created) VALUES (NULL, 42, '2026-09-12T10:15:30')")
            .execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createStatement("SELECT first, created FROM plp_test WHERE id = @P0 /* cursored */")
            .bind("P0", 42)
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("created", LocalDateTime.class)))
            .as(StepVerifier::create)
            .expectNext(LocalDateTime.parse("2026-09-12T10:15:30"))
            .expectComplete()
            .verify(Duration.ofSeconds(30));
    }
}
