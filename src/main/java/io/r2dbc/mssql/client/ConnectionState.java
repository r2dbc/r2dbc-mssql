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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import io.r2dbc.mssql.client.ssl.SslState;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.token.Login7;
import io.r2dbc.mssql.message.token.LoginAckToken;
import io.r2dbc.mssql.message.token.Prelogin;
import io.r2dbc.mssql.message.token.Tabular;
import reactor.netty.Connection;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Connection state according to the TDS state machine. The flow is defined as:
 * <ul>
 * <li>Create the transport connection from the client to the server</li>
 * <li>Enter {@link #PRELOGIN} state and send a {@link Prelogin} message</li>
 * <li>If encryption is required/off/on, then enter {@link #PRELOGIN_SSL_NEGOTIATION}. Note that
 * {@link Prelogin.Encryption#ENCRYPT_OFF} requires SSL negotiation for the {@link Login7} message.</li>
 * <li>Enter {@link #LOGIN} state once SSL is negotiated and send a {@link Login7} message</li>
 * <li>Enter {@link #POST_LOGIN} after receiving login ack</li>
 * </ul>
 * 
 * @author Mark Paluch
 */
public enum ConnectionState {

	/**
	 * State directly after the establishing the transport connection.
	 * <p/>
	 * The only allowed message to send and receive is {@link Prelogin}.
	 */
	PRELOGIN {
		@Override
        BiFunction<Header, ByteBuf, List<? extends Message>> decoder(Client client) {

			return (header, byteBuf) -> {

				assert header.getType() == Type.TABULAR_RESULT;
				assert header.is(Status.StatusBit.EOM);

                return Collections.singletonList(Prelogin.decode(header, byteBuf));
			};
		}

		@Override
		public boolean canAdvance(Message message) {

			Prelogin prelogin = (Prelogin) message;

			Prelogin.Version version = prelogin.getRequiredToken(Prelogin.Version.class);

			if (version.getVersion() >= 9) {
				return true;
			}

			throw new ProtocolException("Unsupported SQL server version: " + version.getVersion());
		}

		@Override
		public ConnectionState next(Message message, Connection connection) {

			Prelogin prelogin = (Prelogin) message;
			Prelogin.Encryption encryption = prelogin.getRequiredToken(Prelogin.Encryption.class);

			if (encryption.requiresLoginSslHanshake()) {

				Channel channel = connection.channel();
				channel.pipeline().fireUserEventTriggered(SslState.LOGIN_ONLY);

				return PRELOGIN_SSL_NEGOTIATION;
			}

			return PRELOGIN;
		}
	},

	/**
	 * SSL negotiation state. This state is handled entirely on the transport level.
	 * 
	 * @see SslHandler
	 */
	PRELOGIN_SSL_NEGOTIATION {

		@Override
		public boolean canAdvance(Message message) {
			return message == SslState.NEGOTIATED;
		}

		@Override
		public ConnectionState next(Message message, Connection connection) {
			return LOGIN;
		}

		@Override
        BiFunction<Header, ByteBuf, List<? extends Message>> decoder(Client client) {
			return (header, byteBuf) -> {

				throw new ProtocolException("Nothing to decode during SSL negotiation");
			};
		}
	},

	LOGIN {

		@Override
		public boolean canAdvance(Message message) {
            return message instanceof Tabular;
        }

		@Override
		public ConnectionState next(Message message, Connection connection) {

            if (message instanceof LoginAckToken) {
				return LOGIN_FAILED;
			}

			return PRELOGIN_SSL_NEGOTIATION;
		}

		@Override
        BiFunction<Header, ByteBuf, List<? extends Message>> decoder(Client client) {
			
			return (header, byteBuf) -> {

				// Expect Tabular message here!
				assert header.getType() == Type.TABULAR_RESULT;
				assert header.is(Status.StatusBit.EOM);

                Tabular tabular = Tabular.decode(byteBuf, client.isColumnEncryptionSupported());
                return tabular.getTokens();
			};
		}
	},
	POST_LOGIN {

		@Override
		public boolean canAdvance(Message message) {
			return false;
		}

		@Override
		public ConnectionState next(Message message, Connection connection) {
			return null;
		}

        @SuppressWarnings("unchecked")
        @Override
        BiFunction<Header, ByteBuf, List<? extends Message>> decoder(Client client) {

            return (header, byteBuf) -> {

                // Expect Tabular message here!
                assert header.getType() == Type.TABULAR_RESULT;

                Tabular tabular = Tabular.decode(byteBuf, client.isColumnEncryptionSupported());
                return tabular.getTokens();
            };
        }
	},

	LOGIN_FAILED {

		@Override
		public boolean canAdvance(Message message) {
			return false;
		}

		@Override
		public ConnectionState next(Message message, Connection connection) {
			return null;
		}

		@Override
        BiFunction<Header, ByteBuf, List<? extends Message>> decoder(Client client) {
			return null;
		}
	};

	public abstract boolean canAdvance(Message message);

	public abstract ConnectionState next(Message message, Connection connection);

    abstract BiFunction<Header, ByteBuf, List<? extends Message>> decoder(Client client);
}
