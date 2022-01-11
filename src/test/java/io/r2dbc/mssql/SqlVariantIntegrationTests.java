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
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * Integration tests for SQL Variant showing that {@code sql_variant} is not supported.
 *
 * @author Mark Paluch
 */
class SqlVariantIntegrationTests extends IntegrationTestSupport {

    @Test
    void shouldExecuteBatch() {

        connection.createStatement(" SELECT SERVERPROPERTY('Edition')").execute()
            .flatMap(mssqlResult -> mssqlResult.map((row, rowMetadata) -> row.get(0)))
            .as(StepVerifier::create)
            .verifyError(UnsupportedOperationException.class);
    }
}
