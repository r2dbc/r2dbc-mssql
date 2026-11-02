/*
 * Copyright 2021 the original author or authors.
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

import io.r2dbc.mssql.codec.Json;
import io.r2dbc.mssql.util.IntegrationTestSupport;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Optional;

/**
 * Integration tests using JSON as return type and native {@code json} columns.
 *
 * @author Mark Paluch
 */
class JsonIntegrationTests extends IntegrationTestSupport {

    @Test
    void shouldExecuteForJsonSimple() {

        connection.createStatement("select 1 as a for json path").execute()
                .flatMap(result -> result.map((row, rowMetadata) -> row.get(0)))
                .as(StepVerifier::create)
                .expectNext("[{\"a\":1}]")
                .verifyComplete();
    }

    @Test
    void shouldExecuteForJsonParametrized() {

        connection.createStatement("select 1 as a where @P0 = @P0 for json path").bind("@P0", true).fetchSize(0).execute()
                .flatMap(result -> result.map((row, rowMetadata) -> row.get(0)))
                .as(StepVerifier::create)
                .expectNext("[{\"a\":1}]")
                .verifyComplete();
    }

    @Test
    void shouldReadNativeJsonColumnAsString() {

        createJsonTable().thenMany(connection.createStatement("SELECT data FROM native_json_test").execute()
                        .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, String.class))))
                .as(StepVerifier::create)
                .expectNext("{\"a\":1}")
                .verifyComplete();
    }

    @Test
    void shouldReadNativeJsonColumnUntyped() {

        createJsonTable().thenMany(connection.createStatement("SELECT data FROM native_json_test").execute()
                        .flatMap(result -> result.map((row, rowMetadata) -> row.get(0))))
                .as(StepVerifier::create)
                .expectNext("{\"a\":1}")
                .verifyComplete();
    }

    @Test
    void shouldReadNativeJsonColumnAsJson() {

        createJsonTable().thenMany(connection.createStatement("SELECT data FROM native_json_test").execute()
                        .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Json.class).asString())))
                .as(StepVerifier::create)
                .expectNext("{\"a\":1}")
                .verifyComplete();
    }

    @Test
    void shouldWriteJsonParameter() {

        createJsonTable()
                .thenMany(connection.createStatement("INSERT INTO native_json_test VALUES (2, @P0)")
                        .bind("@P0", Json.of("{\"b\":2}")).execute().flatMap(MssqlResult::getRowsUpdated))
                .thenMany(connection.createStatement("SELECT data FROM native_json_test WHERE id = 2").execute()
                        .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, String.class))))
                .as(StepVerifier::create)
                .expectNext("{\"b\":2}")
                .verifyComplete();
    }

    @Test
    void shouldReadAndWriteNullJson() {

        createJsonTable()
                .thenMany(connection.createStatement("INSERT INTO native_json_test VALUES (3, @P0)")
                        .bindNull("@P0", Json.class).execute().flatMap(MssqlResult::getRowsUpdated))
                .thenMany(connection.createStatement("SELECT data FROM native_json_test WHERE id = 3").execute()
                        .flatMap(result -> result.map((row, rowMetadata) -> Optional.ofNullable(row.get(0, Json.class)))))
                .as(StepVerifier::create)
                .expectNext(Optional.empty())
                .verifyComplete();
    }

    private Mono<Void> createJsonTable() {

        return Flux.just(
                        "DROP TABLE IF EXISTS native_json_test",
                        "CREATE TABLE native_json_test (id int, data json)",
                        "INSERT INTO native_json_test VALUES (1, '{\"a\":1}')")
                .concatMap(sql -> connection.createStatement(sql).execute().flatMap(MssqlResult::getRowsUpdated))
                .then();
    }

}