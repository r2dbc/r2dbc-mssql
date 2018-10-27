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
package io.r2dbc.mssql.client;

import io.r2dbc.mssql.client.ssl.SslState;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.TDSVersion;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.Login7;
import io.r2dbc.mssql.message.token.Prelogin;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static io.r2dbc.mssql.util.PredicateUtils.or;

/**
 * A utility class that encapsulates the Login message flow.
 */
public final class LoginFlow {

	private LoginFlow() {}

	/**
	 * @param client the {@link Client} to exchange messages with
	 * @param login the login configuration for login negotiation
	 * @return the messages received after authentication is complete, in response to this exchange
	 */
	public static Flux<Message> exchange(Client client, LoginConfiguration login) {

		Objects.requireNonNull(client, "client must not be null");
		Objects.requireNonNull(login, "Login must not be null");

		EmitterProcessor<ClientMessage> requestProcessor = EmitterProcessor.create();
		FluxSink<ClientMessage> requests = requestProcessor.sink();

		Prelogin.PreloginBuilder builder = Prelogin.builder();
		if (login.getConnectionId() != null) {
			builder.withConnectionId(login.getConnectionId());
		}
		AtomicReference<Prelogin> preloginResponse = new AtomicReference<>();

		Prelogin request = builder.build();

		return client.exchange(requestProcessor.startWith(request)) //
            .filter(or(Prelogin.class::isInstance, SslState.class::isInstance, DoneToken.class::isInstance)) //
				.handle((message, sink) -> {

					try {

						if (message instanceof Prelogin) {

							Prelogin response = (Prelogin) message;
							preloginResponse.set(response);

							Prelogin.Encryption encryption = response.getRequiredToken(Prelogin.Encryption.class);

							if (!encryption.requiresSslHanshake()) {
								requests.next(createLoginMessage(login, response));
							}

							return;
						}

						if (message instanceof SslState && message == SslState.NEGOTIATED) {

							Prelogin prelogin = preloginResponse.get();
							requests.next(createLoginMessage(login, prelogin));
							return;
						}

						if (DoneToken.isDone(message)) {
							sink.next(message);
							sink.complete();
							return;
						}

						throw new ProtocolException(String.format("Unexpected login flow message: %s", message));
					} catch (Exception e) {
						requests.error(e);
						sink.error(e);
					}
				});
	}

	private static Login7 createLoginMessage(LoginConfiguration login, Prelogin prelogin) {

		Prelogin.Version serverVersion = prelogin.getRequiredToken(Prelogin.Version.class);
		TDSVersion tdsVersion = getTdsVersion(serverVersion.getVersion());

		return login.asBuilder().tdsVersion(tdsVersion).build();
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

		throw new ProtocolException("Unsupported server version: " + serverVersion);
	}
}
