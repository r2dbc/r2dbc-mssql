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
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Integration tests for multiple active subscriptions.
 *
 * @author Mark Paluch
 */
class ConcurrentAccessIntegrationTests extends IntegrationTestSupport {

    @Test
    void shouldSerializeMultipleActiveSubscriptions() {

        createTable(connection);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();

        Flux<Long> insertOne = insertRecord(connection, 1);
        Flux<Long> insertTwo = insertRecord(connection, 2);
        Flux<Long> insertThree = insertRecord(connection, 3);

        Flux.merge(insertOne, insertTwo, insertThree).as(StepVerifier::create).expectNextCount(3).verifyComplete();

        connection.commitTransaction().as(StepVerifier::create).verifyComplete();

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
            .expectNextCount(3)
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

    private Flux<Long> insertRecord(MssqlConnection connection, int id) {

        return connection.createStatement("INSERT INTO r2dbc_example VALUES(@id, @firstname, @lastname)")
            .bind("id", id)
            .bind("firstname", "Walter")
            .bind("lastname", "White")
            .execute()
            .flatMap(Result::getRowsUpdated);
    }
}
