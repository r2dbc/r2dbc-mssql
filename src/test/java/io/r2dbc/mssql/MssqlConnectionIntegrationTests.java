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

import io.r2dbc.mssql.util.IntegrationTestSupport;
import io.r2dbc.spi.ColumnMetadata;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.ConnectException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link MssqlConnection} and {@link MssqlStatement}.
 *
 * @author Mark Paluch
 */
class MssqlConnectionIntegrationTests extends IntegrationTestSupport {

    @Test
    void shouldFailOnConnectionRefused() {

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
                .host(SERVER.getHost())
                .port(123)
                .username(SERVER.getUsername())
                .password(SERVER.getPassword())
                .build();

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(configuration);

        connectionFactory.create()
                .as(StepVerifier::create)
                .expectError(ConnectException.class)
                .verify();
    }

    @Test
    void shouldFailOnLoginFailedRefused() {

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
                .host(SERVER.getHost())
                .port(SERVER.getPort())
                .username(SERVER.getUsername())
                .password("foobar")
                .build();

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(configuration);

        connectionFactory.create()
                .as(StepVerifier::create)
                .expectError(MssqlException.class)
                .verify();
    }

    @Test
    void shouldInsertAndSelectUsingMap() {

        createTable(connection);

        insertRecord(1);

        connection.createStatement("SELECT * FROM r2dbc_example ORDER BY first_name")
                .execute()
                .flatMap(it -> it.map((row, rowMetadata) -> {

                    Map<String, Object> values = new LinkedHashMap<>();

                    for (ColumnMetadata column : rowMetadata.getColumnMetadatas()) {
                        values.put(column.getName(), row.get(column.getName()));
                    }

                    return values;
                }))
                .as(StepVerifier::create)
                .consumeNextWith(actual -> {

                    assertThat(actual)
                            .containsEntry("id", 1)
                            .containsEntry("first_name", "Walter")
                            .containsEntry("last_name", "White");
                })
                .verifyComplete();
    }

    @Test
    void shouldInsertAndSelectUsingRowCount() {

        createTable(connection);

        insertRecord(1);
        insertRecord(2);
        insertRecord(3);

        connection.createStatement("SELECT * FROM r2dbc_example")
                .execute()
                .flatMap(MssqlResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(3)
                .verifyComplete();
    }

    @Test
    void shouldInsertAndSelectUsingPaging() {

        createTable(connection);

        insertRecord(1);
        insertRecord(2);
        insertRecord(3);

        connection.createStatement("SELECT * FROM r2dbc_example ORDER BY id OFFSET @Offset ROWS" +
                "  FETCH NEXT @Rows ROWS ONLY")
                .bind("Offset", 0)
                .bind("Rows", 2)
                .execute()
                .flatMap(it -> it.map((row, rowMetadata) -> row.get("id", Integer.class)))
                .as(StepVerifier::create)
                .expectNext(1)
                .expectNext(2)
                .verifyComplete();

        connection.createStatement("SELECT * FROM r2dbc_example ORDER BY id OFFSET @Offset ROWS" +
                " FETCH NEXT @Rows ROWS ONLY")
                .bind("Offset", 2)
                .bind("Rows", 2)
                .execute()
                .flatMap(it -> it.map((row, rowMetadata) -> row.get("id", Integer.class)))
                .as(StepVerifier::create)
                .expectNext(3)
                .verifyComplete();
    }

    @Test
    void shouldInsertAndSelectCompoundStatement() {

        createTable(connection);

        connection.createStatement("SELECT * FROM r2dbc_example;SELECT * FROM r2dbc_example")
                .execute()
                .flatMap(it -> it.map((row, rowMetadata) -> {
                    return new Object();   // just a marker
                }).collectList())
                .as(StepVerifier::create)
                .consumeNextWith(actual -> assertThat(actual).isEmpty())
                .consumeNextWith(actual -> assertThat(actual).isEmpty())
                .verifyComplete();

        insertRecord(1);

        connection.createStatement("SELECT * FROM r2dbc_example;SELECT * FROM r2dbc_example")
                .execute()
                .flatMap(it -> it.map((row, rowMetadata) -> {

                    Map<String, Object> values = new LinkedHashMap<>();

                    for (ColumnMetadata column : rowMetadata.getColumnMetadatas()) {
                        values.put(column.getName(), row.get(column.getName()));
                    }

                    return values;
                }).collectList())
                .as(StepVerifier::create)
                .consumeNextWith(actual -> assertThat(actual).hasSize(1))
                .consumeNextWith(actual -> assertThat(actual).hasSize(1))
                .verifyComplete();
    }

    @Test
    void shouldReusePreparedStatements() {

        createTable(connection);

        insertRecord(1);
        insertRecord(2);
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

    private void insertRecord(int id) {

        connection.createStatement("INSERT INTO r2dbc_example VALUES(@id, @firstname, @lastname)")
                .bind("id", id)
                .bind("firstname", "Walter")
                .bind("lastname", "White")
                .execute()
                .flatMap(MssqlResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();
    }
}
