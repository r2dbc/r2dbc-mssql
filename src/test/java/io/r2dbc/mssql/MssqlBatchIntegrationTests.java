/*
 * Copyright 2018-2022 the original author or authors.
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link MssqlBatch}.
 *
 * @author Mark Paluch
 */
class MssqlBatchIntegrationTests extends IntegrationTestSupport {

    static {
        Hooks.onOperatorDebug();
    }

    @Test
    void shouldRunBatchWithMultipleResults() {

        AtomicInteger resultCounter = new AtomicInteger();
        AtomicInteger firstUpdateCount = new AtomicInteger();
        AtomicInteger rowCount = new AtomicInteger();

        Flux.from(connection.createBatch().add("DECLARE @t TABLE(i INT)").add("INSERT INTO @t VALUES (1),(2),(3)").add("SELECT * FROM @t")
            .execute()).flatMap(it -> {

            if (resultCounter.compareAndSet(0, 1)) {
                return it.getRowsUpdated().doOnNext(firstUpdateCount::set).then();
            }

            if (resultCounter.incrementAndGet() == 2) {
                return it.map(((row, rowMetadata) -> {
                    rowCount.incrementAndGet();

                    return new Object();
                })).then();
            }

            throw new IllegalStateException("Unexpected result");
        }).as(StepVerifier::create).verifyComplete();

        assertThat(resultCounter).hasValue(2);
        assertThat(firstUpdateCount).hasValue(3);
        assertThat(rowCount).hasValue(3);
    }

}
