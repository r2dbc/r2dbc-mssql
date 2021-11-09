/*
 * Copyright 2019-2021 the original author or authors.
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
 * Integration tests using XML as return type.
 *
 * @author Mark Paluch
 */
class XmlIntegrationTests extends IntegrationTestSupport {

    @Test
    void shouldExecuteForXmlSimple() {

        connection.createStatement("select 1 as a for xml path").execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0)))
            .as(StepVerifier::create)
            .expectNext("<row><a>1</a></row>")
            .verifyComplete();
    }

    @Test
    void shouldExecuteForXmlParametrized() {

        connection.createStatement("select 1 as a where @P0 = @P0 for xml path").bind("@P0", true).fetchSize(0).execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0)))
            .as(StepVerifier::create)
            .expectNext("<row><a>1</a></row>")
            .verifyComplete();
    }

}
