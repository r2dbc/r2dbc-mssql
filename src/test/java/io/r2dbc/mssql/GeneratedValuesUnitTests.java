/*
 * Copyright 2019 the original author or authors.
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

import io.r2dbc.mssql.client.ssl.SslState;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ReturnStatus;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link GeneratedValues}.
 *
 * @author Mark Paluch
 */
class GeneratedValuesUnitTests {

    @Test
    void shouldRejectNullColumns() {

        assertThatThrownBy(() -> GeneratedValues.getGeneratedKeysClause((String[]) null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GeneratedValues.getGeneratedKeysClause(new String[]{null})).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectMultipleColumns() {

        assertThatThrownBy(() -> GeneratedValues.getGeneratedKeysClause("foo", "bar")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldAugmentQuery() {
        assertThat(GeneratedValues.augmentQuery("foo", new String[]{"bar"})).isEqualTo("foo SELECT SCOPE_IDENTITY() AS bar");
    }

    @Test
    void shouldReportGeneratedKeysExpectation() {
        assertThat(GeneratedValues.shouldExpectGeneratedKeys(null)).isFalse();
        assertThat(GeneratedValues.shouldExpectGeneratedKeys(new String[0])).isTrue();
    }

    @Test
    void shouldGenerateDefaultClause() {
        assertThat(GeneratedValues.getGeneratedKeysClause()).isEqualTo("SELECT SCOPE_IDENTITY() AS GENERATED_KEYS");
    }

    @Test
    void shouldGenerateCustomizedClause() {
        assertThat(GeneratedValues.getGeneratedKeysClause("foo")).isEqualTo("SELECT SCOPE_IDENTITY() AS foo");
    }

    @Test
    void shouldReorderMessageFlowForGeneratedKeys() {

        ReturnStatus status = ReturnStatus.create(2);
        DoneToken count = DoneToken.count(10);

        Flux.just(status, count, SslState.CONNECTION, DoneToken.create(1)) //
            .transform(GeneratedValues::reduceToSingleCountDoneToken) //
            .as(StepVerifier::create) //
            .expectNext(status, SslState.CONNECTION, count) //
            .verifyComplete();
    }
}
