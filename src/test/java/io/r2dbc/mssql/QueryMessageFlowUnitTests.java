/*
 * Copyright 2019-2020 the original author or authors.
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

import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.client.ConnectionContext;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.token.DoneInProcToken;
import io.r2dbc.mssql.message.token.DoneProcToken;
import io.r2dbc.mssql.message.token.DoneToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.function.Predicate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link QueryMessageFlow}.
 *
 * @author Mark Paluch
 */
@SuppressWarnings("unchecked")
class QueryMessageFlowUnitTests {

    Client client = mock(Client.class);

    @BeforeEach
    void setUp() {
        when(client.getTransactionDescriptor()).thenReturn(TransactionDescriptor.empty());
        when(client.getContext()).thenReturn(new ConnectionContext());
    }

    @Test
    void shouldAwaitDoneProcTokenShouldNotCompleteFlow() {

        when(client.exchange(any(Publisher.class), any(Predicate.class))).thenReturn(Flux.just(DoneToken.more(20), DoneProcToken.create(0), DoneInProcToken.create(0)));

        QueryMessageFlow.exchange(client, "foo")
            .as(StepVerifier::create)
            .expectNext(DoneToken.more(20), DoneProcToken.create(0), DoneInProcToken.create(0))
            .thenCancel()
            .verify();
    }

    @Test
    void shouldAwaitDoneToken() {

        when(client.exchange(any(Publisher.class), any(Predicate.class))).thenReturn(Flux.just(DoneInProcToken.create(0), DoneToken.create(0)));

        QueryMessageFlow.exchange(client, "foo")
            .as(StepVerifier::create)
            .expectNext(DoneInProcToken.create(0), DoneToken.create(0))
            .verifyComplete();
    }
}
