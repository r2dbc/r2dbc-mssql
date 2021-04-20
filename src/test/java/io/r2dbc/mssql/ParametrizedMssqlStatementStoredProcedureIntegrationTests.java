/*
 * Copyright 2018-2021 the original author or authors.
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
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import reactor.test.StepVerifier;

/**
 * Integration tests for {@link ParametrizedMssqlStatement} to call stored procedures.
 *
 * @author Mark Paluch
 */
class ParametrizedMssqlStatementStoredProcedureIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void setUp() {

        try {
            SERVER.getJdbcOperations().execute("DROP PROCEDURE test_proc");
        } catch (DataAccessException ignore) {
        }

        SERVER.getJdbcOperations().execute("CREATE PROCEDURE test_proc\n" +
            "    @TheName nvarchar(50),\n" +
            "    @Greeting nvarchar(255) OUTPUT\n" +
            "AS\n" +
            "\n" +
            "    SET NOCOUNT ON;  \n" +
            "    SET @Greeting = CONCAT('Hello ', @TheName)");
    }

    @Test
    void shouldCallProcedure() {

        connection.createStatement("EXEC test_proc @P0, @Greeting OUTPUT")
            .bind("@P0", "Walter")
            .bind("@Greeting", Parameters.out(R2dbcType.VARCHAR))
            .execute()
            .flatMap(it -> it.map((row, metadata) -> {
                return row.get(0);
            }))
            .as(StepVerifier::create)
            .expectNext("Hello Walter")
            .verifyComplete();
    }

    @Test
    void shouldCallProcedureWithFetchSize() {

        connection.createStatement("EXEC test_proc @P0, @Greeting OUTPUT")
            .fetchSize(256)
            .bind("@P0", "Walter")
            .bind("@Greeting", Parameters.out(R2dbcType.VARCHAR))
            .execute()
            .flatMap(it -> it.map((row, metadata) -> {
                return row.get(0);
            }))
            .as(StepVerifier::create)
            .expectNext("Hello Walter")
            .verifyComplete();
    }

}
