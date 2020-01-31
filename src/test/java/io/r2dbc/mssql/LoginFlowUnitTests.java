/*
 * Copyright 2018-2020 the original author or authors.
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

import io.r2dbc.mssql.client.LoginExchangeResult;
import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.EnvChangeToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.Prelogin;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
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

        LoginConfiguration login = new LoginConfiguration("app", null, "db", "host", "bar", "server", false, "foo");

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

        LoginConfiguration login = new LoginConfiguration("app", null, "db", "host", "bar", "server", false, "foo");

        LoginFlow.exchange(client, login)
            .as(StepVerifier::create)
            .expectNext(LoginExchangeResult.connected())
            .verifyComplete();
    }

    @Test
    void shouldFinishWithRoute() {

        byte[] routingDataValue = new byte[21];

        HexUtils.decodeToByteBuf("13000039300700740065007300740069006e006700").readBytes(routingDataValue);

        EnvChangeToken routingToken = new EnvChangeToken(22, EnvChangeToken.EnvChangeType.Routing, routingDataValue,
            null);

        TestClient client = TestClient.builder()
            .assertNextRequestWith(actual -> assertThat(actual).isInstanceOf(Prelogin.class))
            .thenRespond(routingToken, DoneToken.create(0))
            .build();

        LoginConfiguration login = new LoginConfiguration("app", null, "db", "host", "bar", "server", false, "foo");

        LoginFlow.exchange(client, login).as(StepVerifier::create).expectNextMatches(result -> {
            return result.getOutcome() == LoginExchangeResult.Outcome.ROUTED && "testing".equals(
                result.getAlternateServerName()) && result.getAlternateServerPort() == 12345;
        }).verifyComplete();
    }

    @Test
    void shouldPropagateError() {

        TestClient client = TestClient.builder()
            .assertNextRequestWith(actual -> assertThat(actual).isInstanceOf(Prelogin.class))
            .thenRespond(new ErrorToken(0, 0, (byte) 0x00, (byte) 0x0E, "some error", "", "", 0))
            .expectClose()
            .build();

        LoginConfiguration login = new LoginConfiguration("app", null, "db", "host", "bar", "server", false, "foo");

        LoginFlow.exchange(client, login)
            .as(StepVerifier::create)
            .expectError(R2dbcPermissionDeniedException.class)
            .verify();
    }
}
