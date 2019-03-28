/*
 * Copyright 2018-2019 the original author or authors.
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

import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.SqlBatch;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * Unit tests for {@link MssqlBatch}.
 *
 * @author Mark Paluch
 */
class MssqlBatchUnitTests {

    @Test
    void shouldExecuteSingleBatch() {

        TestClient client = TestClient.builder()
            .expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "foo"))
            .thenRespond(DoneToken.create(1))
            .build();

        new MssqlBatch(client, new ConnectionOptions())
            .add("foo")
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void shouldExecuteMultiBatch() {

        TestClient client = TestClient.builder()
            .expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "foo; bar"))
            .thenRespond(DoneToken.create(1), DoneToken.create(1))
            .build();

        new MssqlBatch(client, new ConnectionOptions())
            .add("foo")
            .add("bar")
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void shouldFailOnExecution() {

        TestClient client = TestClient.builder()
            .expectRequest(SqlBatch.create(1, TransactionDescriptor.empty(), "foo"))
            .thenRespond(new ErrorToken(1, 1, (byte) 0, (byte) 0, "error", "server",
                "proc", 0))
            .build();

        new MssqlBatch(client, new ConnectionOptions())
            .add("foo")
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }
}
