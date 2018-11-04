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

import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.Prelogin;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LoginFlow}.
 *
 * @author Mark Paluch
 */
class LoginFlowUnitTests {

    @Test
    void shouldInitiateLogin() {

        List<Prelogin.Token> tokens = new ArrayList<>();

        tokens.add(new Prelogin.Version(14, 0));
        tokens.add(new Prelogin.Encryption(Prelogin.Encryption.ENCRYPT_NOT_SUP));
        tokens.add(Prelogin.Terminator.INSTANCE);
        Prelogin response = new Prelogin(tokens);

        TestClient client = TestClient.builder()
            .assertNextRequestWith(actual -> assertThat(actual).isInstanceOf(Prelogin.class))
            .thenRespond(response)
            .build();

        LoginConfiguration login = new LoginConfiguration("foo", "bar", "db", "host", "app", "server", null);

        LoginFlow.exchange(client, login)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldFinishLogin() {

        TestClient client = TestClient.builder()
            .assertNextRequestWith(actual -> assertThat(actual).isInstanceOf(Prelogin.class))
            .thenRespond(DoneToken.create(0))
            .build();

        LoginConfiguration login = new LoginConfiguration("foo", "bar", "db", "host", "app", "server", null);

        LoginFlow.exchange(client, login)
            .as(StepVerifier::create)
            .expectNext(DoneToken.create(0))
            .verifyComplete();
    }

    @Test
    void shouldPropagateError() {

        TestClient client = TestClient.builder()
            .assertNextRequestWith(actual -> assertThat(actual).isInstanceOf(Prelogin.class))
            .thenRespond(new ErrorToken(0, 0, (byte) 0x00, (byte) 0x25, "some error", "", "", 0))
            .expectClose()
            .build();

        LoginConfiguration login = new LoginConfiguration("foo", "bar", "db", "host", "app", "server", null);

        LoginFlow.exchange(client, login)
            .as(StepVerifier::create)
            .expectError(MssqlException.class)
            .verify();
    }
}
