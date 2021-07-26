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
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * Integration tests for {@link SimpleMssqlStatement}.
 *
 * @author Mark Paluch
 */
class SimpleMssqlStatementIntegrationTests extends IntegrationTestSupport {

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

}
