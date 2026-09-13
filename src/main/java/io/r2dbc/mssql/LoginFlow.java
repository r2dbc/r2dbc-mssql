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

import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.client.ssl.SslState;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.TDSVersion;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.Login7;
import io.r2dbc.mssql.message.token.Prelogin;
import io.r2dbc.mssql.message.token.SspiMessage;
import io.r2dbc.mssql.message.token.SspiToken;
import io.r2dbc.mssql.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.util.concurrent.atomic.AtomicReference;

import static io.r2dbc.mssql.util.PredicateUtils.or;

/**
 * A utility class that encapsulates the Login message flow.
 *
 * @author Mark Paluch
 */
final class LoginFlow {

    private LoginFlow() {
    }

    /**
     * @param client the {@link Client} to exchange messages with
     * @param login  the login configuration for login negotiation
     * @return the messages received after authentication is complete, in response to this exchange
     */
    static Flux<Message> exchange(Client client, LoginConfiguration login) {

        Assert.requireNonNull(client, "client must not be null");
        Assert.requireNonNull(login, "Login must not be null");

        return exchange0(client, login, null);
    }

    /**
     * Exchange login messages using integrated authentication.
     *
     * @param client         the {@link Client} to exchange messages with
     * @param login          the login configuration for login negotiation
     * @param authentication the integrated authentication provider
     * @return the messages received after authentication is complete, in response to this exchange
     */
    static Flux<Message> exchange(Client client, LoginConfiguration login, IntegratedAuthentication authentication) {

        Assert.requireNonNull(client, "client must not be null");
        Assert.requireNonNull(login, "Login must not be null");
        Assert.requireNonNull(authentication, "Integrated authentication must not be null");

        return Flux.usingWhen(Mono.just(authentication),
            it -> exchange0(client, login, it),
            IntegratedAuthentication::close);
    }

    private static Flux<Message> exchange0(Client client, LoginConfiguration login,
                                           @Nullable IntegratedAuthentication authentication) {

        Prelogin.Builder builder = Prelogin.builder();
        if (login.getConnectionId() != null) {
            builder.withConnectionId(login.getConnectionId());
        }

        if (login.useSsl()) {
            builder.withEncryptionEnabled();
        }

        AtomicReference<Prelogin> preloginResponse = new AtomicReference<>();
        Sinks.Many<Mono<? extends ClientMessage>> requests = Sinks.many().unicast().onBackpressureBuffer();

        Prelogin request = builder.build();
        requests.emitNext(Mono.just(request), Sinks.EmitFailureHandler.FAIL_FAST);

        Flux<ClientMessage> requestMessages = requests.asFlux().concatMap(it -> it);

        return client.exchange(requestMessages, DoneToken::isDone) //
            .filter(or(Prelogin.class::isInstance, SslState.class::isInstance, SspiToken.class::isInstance,
                DoneToken.class::isInstance, ErrorToken.class::isInstance)) //
            .handle((message, sink) -> {

                try {

                    if (message instanceof Prelogin) {

                        Prelogin response = (Prelogin) message;
                        preloginResponse.set(response);

                        Prelogin.Encryption encryption = response.getRequiredToken(Prelogin.Encryption.class);

                        if (login.useSsl() && !encryption.requiresConnectionSslHandshake()) {
                            sink.error(new ProtocolException(String.format("SSL encryption was requested but the server does not support encryption (%s) of the entire connection. Closing connection.",
                                encryption.getEncryptionFlagName())));
                            client.close().subscribe();
                            return;
                        }

                        if (!encryption.requiresSslHandshake()) {
                            emitLoginRequest(requests, login, response, authentication);
                        }

                        return;
                    }

                    if (message instanceof SslState && message == SslState.NEGOTIATED) {

                        Prelogin prelogin = preloginResponse.get();
                        emitLoginRequest(requests, login, prelogin, authentication);
                        return;
                    }

                    if (message instanceof SspiToken) {

                        if (authentication == null) {
                            throw ProtocolException.unsupported("Received an SSPI challenge without integrated authentication");
                        }

                        SspiToken token = (SspiToken) message;

                        Mono<? extends ClientMessage> response = Mono.defer(() ->
                                authentication.nextToken(token.getSspiBuffer()))
                            .map(SspiMessage::create);

                        requests.emitNext(response, Sinks.EmitFailureHandler.FAIL_FAST);
                        return;
                    }

                    if (DoneToken.isDone(message)) {
                        sink.next(message);
                        sink.complete();

                        return;
                    }

                    if (message instanceof ErrorToken) {
                        sink.error(ExceptionFactory.createException((ErrorToken) message, ""));
                        client.close().subscribe();
                        return;
                    }

                    throw ProtocolException.unsupported(String.format("Unexpected login flow message: %s", message));
                } catch (Exception e) {
                    requests.emitError(e, Sinks.EmitFailureHandler.FAIL_FAST);
                    sink.error(e);
                }
            });
    }

    private static void emitLoginRequest(Sinks.Many<Mono<? extends ClientMessage>> requests,
                                         LoginConfiguration login, Prelogin prelogin,
                                         @Nullable IntegratedAuthentication authentication) {

        if (authentication == null) {
            requests.emitNext(Mono.just(createLoginMessage(login, prelogin)), Sinks.EmitFailureHandler.FAIL_FAST);
            return;
        }

        Mono<? extends ClientMessage> request = Mono.defer(authentication::initialToken)
            .switchIfEmpty(Mono.error(new IllegalStateException(
                "Integrated authentication did not produce an initial SSPI token")))
            .map(sspiBuffer -> createLoginMessage(login, prelogin, sspiBuffer));

        requests.emitNext(request, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    private static Login7 createLoginMessage(LoginConfiguration login, Prelogin prelogin) {

        Prelogin.Version serverVersion = prelogin.getRequiredToken(Prelogin.Version.class);
        TDSVersion tdsVersion = getTdsVersion(serverVersion.getVersion());

        return login.asBuilder().tdsVersion(tdsVersion).build();
    }

    private static Login7 createLoginMessage(LoginConfiguration login, Prelogin prelogin, byte[] sspiBuffer) {

        Prelogin.Version serverVersion = prelogin.getRequiredToken(Prelogin.Version.class);
        TDSVersion tdsVersion = getTdsVersion(serverVersion.getVersion());

        return login.asBuilder().tdsVersion(tdsVersion).integratedSecurity(sspiBuffer).build();
    }

    private static TDSVersion getTdsVersion(int serverVersion) {

        if (serverVersion >= 11) // Denali --> TDS 7.4
        {
            return TDSVersion.VER_DENALI;
        }

        if (serverVersion >= 10) // Katmai (10.0) & later 7.3B
        {
            return TDSVersion.VER_KATMAI;
        }

        if (serverVersion >= 9) // Yukon (9.0) --> TDS 7.2 // Prelogin disconnects anything older
        {
            return TDSVersion.VER_YUKON;
        }

        throw ProtocolException.unsupported("Unsupported server version: " + serverVersion);
    }

}
