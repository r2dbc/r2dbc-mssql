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

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import io.r2dbc.mssql.client.ssl.SslState;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.token.AbstractDoneToken;
import io.r2dbc.mssql.message.token.Login7;
import io.r2dbc.mssql.message.token.Prelogin;
import io.r2dbc.mssql.message.token.Tabular;
import io.r2dbc.mssql.util.Assert;
import reactor.netty.Connection;

import java.util.Collections;

import static io.r2dbc.mssql.message.header.Status.StatusBit;

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
 * Connection states can {@link #canAdvance(Message) advance} triggered by a received {@link Message}. A state can provide a {@link MessageDecoder} function to decode messages exchanged in that 
 * state. Note that message decoding is not supported in all states as per TDS state specification.
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
        MessageDecoder decoder(Client client) {

            return (header, byteBuf) -> {

                Assert.isTrue(header.getType() == Type.TABULAR_RESULT, () -> "Expected tabular message, header type is: " + header.getType());
                Assert.isTrue(header.is(StatusBit.EOM), "Prelogin response packet must not be chunked");

                return Collections.singletonList(Prelogin.decode(byteBuf));
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
        MessageDecoder decoder(Client client) {
            return (header, byteBuf) -> {
                throw new ProtocolException("Nothing to decode during SSL negotiation");
            };
        }
    },

    /**
     * State during login.
     *
     * @see Login7
     */
    LOGIN {
        @Override
        public boolean canAdvance(Message message) {
            return message instanceof AbstractDoneToken;
        }

        @Override
        public ConnectionState next(Message message, Connection connection) {

            if (AbstractDoneToken.isDone(message)) {
                return POST_LOGIN;
            }

            return LOGIN_FAILED;
        }

        @Override
        MessageDecoder decoder(Client client) {

            return (header, byteBuf) -> {

                Assert.isTrue(header.getType() == Type.TABULAR_RESULT, () -> "Expected tabular message, header type is: " + header.getType());
                Assert.isTrue(header.is(StatusBit.EOM), "Login response packet must not be chunked");

                Tabular tabular = Tabular.decode(byteBuf, client.isColumnEncryptionSupported());
                return tabular.getTokens();
            };
        }
    },

    /**
     * State after successful login.
     */
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
        MessageDecoder decoder(Client client) {

            Tabular.TabularDecoder decoder = Tabular.createDecoder(client.isColumnEncryptionSupported());

            return (header, byteBuf) -> {

                Assert.isTrue(header.getType() == Type.TABULAR_RESULT, () -> "Expected tabular message, header type is: " + header.getType());

                return decoder.decode(byteBuf);
            };
        }
    },

    /**
     * State after failed login.
     */
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
        MessageDecoder decoder(Client client) {
            return null;
        }
    };

    /**
     * Check whether the state can advance from the given {@link Message} into a differen {@link ConnectionState}.
     *
     * @param message the message to inspect.
     * @return {@literal true} if the state can advance.
     */
    public abstract boolean canAdvance(Message message);

    /**
     * Return the next {@link ConnectionState} using the given {@link Message} and {@link Connection transport connection}.
     *
     * @param message    the message that triggered connection state change.
     * @param connection the transport connection.
     * @return the next {@link ConnectionState}.
     */
    public abstract ConnectionState next(Message message, Connection connection);

    /**
     * Returns the {@link MessageDecoder} that is applicable for the current {@link ConnectionState}.
     * Message decoding is not supported in all states.
     *
     * @param client the client instance.
     * @return the {@link MessageDecoder}.
     */
    abstract MessageDecoder decoder(Client client);
}
