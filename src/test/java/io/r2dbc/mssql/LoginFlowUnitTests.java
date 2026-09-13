/*
 * Copyright 2018 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.message.tds.ContextualTdsFragment;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.Login7;
import io.r2dbc.mssql.message.token.Prelogin;
import io.r2dbc.mssql.message.token.SspiMessage;
import io.r2dbc.mssql.message.token.SspiToken;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
            .expectNext(DoneToken.create(0))
            .verifyComplete();
    }

    @Test
    void shouldFailWhenServerDoesNotSupportRequestedEncryption() {

        List<Prelogin.Token> tokens = new ArrayList<>();

        tokens.add(new Prelogin.Version(14, 0));
        tokens.add(new Prelogin.Encryption(Prelogin.Encryption.ENCRYPT_NOT_SUP));
        tokens.add(Prelogin.Terminator.INSTANCE);
        Prelogin response = new Prelogin(tokens);

        TestClient client = TestClient.builder()
            .assertNextRequestWith(actual -> assertThat(actual).isInstanceOf(Prelogin.class))
            .thenRespond(response)
            .expectClose()
            .build();

        LoginConfiguration login = new LoginConfiguration("app", null, "db", "host", "bar", "server", true, "foo");

        LoginFlow.exchange(client, login)
            .as(StepVerifier::create)
            .expectError(ProtocolException.class)
            .verify();
    }


    @Test
    void shouldNegotiateIntegratedAuthenticationReactively() {

        List<Prelogin.Token> tokens = new ArrayList<>();

        tokens.add(new Prelogin.Version(14, 0));
        tokens.add(new Prelogin.Encryption(Prelogin.Encryption.ENCRYPT_NOT_SUP));
        tokens.add(Prelogin.Terminator.INSTANCE);
        Prelogin preloginResponse = new Prelogin(tokens);

        byte[] initialToken = new byte[]{0x60, 0x01, 0x02};
        byte[] challenge1 = new byte[]{0x11, 0x12};
        byte[] response1 = new byte[]{0x21, 0x22, 0x23};
        byte[] challenge2 = new byte[]{0x31, 0x32, 0x33};
        byte[] response2 = new byte[]{0x41, 0x42};

        SspiToken serverChallenge1 = sspiToken(challenge1);
        SspiToken serverChallenge2 = sspiToken(challenge2);

        AtomicInteger round = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();

        IntegratedAuthentication authentication = new IntegratedAuthentication() {

            @Override
            public Mono<byte[]> initialToken() {
                return Mono.just(initialToken);
            }

            @Override
            public Mono<byte[]> nextToken(byte[] serverToken) {

                int currentRound = round.getAndIncrement();

                if (currentRound == 0) {
                    assertThat(serverToken).containsExactly(challenge1);
                    return Mono.just(response1);
                }

                assertThat(currentRound).isEqualTo(1);
                assertThat(serverToken).containsExactly(challenge2);
                return Mono.just(response2);
            }

            @Override
            public Mono<Void> close() {
                return Mono.fromRunnable(() -> closed.set(true));
            }
        };

        TestClient client = TestClient.builder()
            .window()
            .assertNextRequestWith(actual -> assertThat(actual).isInstanceOf(Prelogin.class))
            .thenRespond(preloginResponse)
            .assertNextRequestWith(actual -> {
                assertThat(actual).isInstanceOf(Login7.class);

                ContextualTdsFragment fragment =
                    (ContextualTdsFragment) ((Login7) actual).encode(TestByteBufAllocator.TEST, 0);
                ByteBuf buffer = fragment.getByteBuf();

                assertThat(buffer.getUnsignedByte(25) & 0x80).isEqualTo(0x80);

                int sspiOffset = buffer.getUnsignedShortLE(78);
                int sspiLength = buffer.getUnsignedShortLE(80);

                assertThat(ByteBufUtil.getBytes(buffer, sspiOffset, sspiLength)).containsExactly(initialToken);
            })
            .thenRespond(serverChallenge1)
            .assertNextRequestWith(actual -> assertSspiMessage(actual, response1))
            .thenRespond(serverChallenge2)
            .assertNextRequestWith(actual -> assertSspiMessage(actual, response2))
            .thenRespond(DoneToken.create(0))
            .done()
            .build();

        LoginConfiguration login = new LoginConfiguration("app", null, "db", "host", "bar", "server", false, "foo");

        LoginFlow.exchange(client, login, authentication)
            .as(StepVerifier::create)
            .expectNext(DoneToken.create(0))
            .verifyComplete();

        assertThat(round).hasValue(2);
        assertThat(closed).isTrue();
    }

    @Test
    void shouldCompleteIntegratedAuthenticationWithoutFinalClientToken() {

        List<Prelogin.Token> tokens = new ArrayList<>();

        tokens.add(new Prelogin.Version(14, 0));
        tokens.add(new Prelogin.Encryption(Prelogin.Encryption.ENCRYPT_NOT_SUP));
        tokens.add(Prelogin.Terminator.INSTANCE);
        Prelogin preloginResponse = new Prelogin(tokens);

        byte[] initialToken = new byte[]{0x60, 0x01, 0x02};
        byte[] challenge = new byte[]{0x11, 0x12};
        byte[] response = new byte[]{0x21, 0x22, 0x23};
        byte[] finalServerToken = new byte[]{0x31, 0x32, 0x33};

        SspiToken serverChallenge = sspiToken(challenge);
        SspiToken serverFinalToken = sspiToken(finalServerToken);

        AtomicInteger round = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();

        IntegratedAuthentication authentication = new IntegratedAuthentication() {

            @Override
            public Mono<byte[]> initialToken() {
                return Mono.just(initialToken);
            }

            @Override
            public Mono<byte[]> nextToken(byte[] serverToken) {

                int currentRound = round.getAndIncrement();

                if (currentRound == 0) {
                    assertThat(serverToken).containsExactly(challenge);
                    return Mono.just(response);
                }

                assertThat(currentRound).isEqualTo(1);
                assertThat(serverToken).containsExactly(finalServerToken);
                return Mono.empty();
            }

            @Override
            public Mono<Void> close() {
                return Mono.fromRunnable(() -> closed.set(true));
            }
        };

        TestClient client = TestClient.builder()
            .window()
            .assertNextRequestWith(actual -> assertThat(actual).isInstanceOf(Prelogin.class))
            .thenRespond(preloginResponse)
            .assertNextRequestWith(actual -> assertThat(actual).isInstanceOf(Login7.class))
            .thenRespond(serverChallenge)
            .assertNextRequestWith(actual -> assertSspiMessage(actual, response))
            .thenRespond(serverFinalToken, DoneToken.create(0))
            .done()
            .build();

        LoginConfiguration login = new LoginConfiguration("app", null, "db", "host", "bar", "server", false, "foo");

        LoginFlow.exchange(client, login, authentication)
            .as(StepVerifier::create)
            .expectNext(DoneToken.create(0))
            .verifyComplete();

        assertThat(round).hasValue(2);
        assertThat(closed).isTrue();
    }

    private static SspiToken sspiToken(byte[] payload) {

        ByteBuf buffer = Unpooled.buffer(payload.length + 2);
        buffer.writeShortLE(payload.length);
        buffer.writeBytes(payload);

        return SspiToken.decode(buffer);
    }

    private static void assertSspiMessage(Object actual, byte[] expectedPayload) {

        assertThat(actual).isInstanceOf(SspiMessage.class);

        ContextualTdsFragment fragment =
            (ContextualTdsFragment) ((SspiMessage) actual).encode(TestByteBufAllocator.TEST, 0);

        assertThat(ByteBufUtil.getBytes(fragment.getByteBuf())).containsExactly(expectedPayload);
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
