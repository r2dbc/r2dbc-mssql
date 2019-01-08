/*
 * Copyright 2018-2019 the original author or authors.
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
import io.netty.channel.ChannelPipeline;
import io.r2dbc.mssql.client.ssl.SslState;
import io.r2dbc.mssql.message.token.Prelogin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.netty.Connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConnectionState}.
 *
 * @author Mark Paluch
 */
class ConnectionStateUnitTests {

    Connection nettyConnection = mock(Connection.class);

    Channel channel = mock(Channel.class);

    ChannelPipeline pipeline = mock(ChannelPipeline.class);

    @BeforeEach
    void setUp() {

        when(nettyConnection.channel()).thenReturn(channel);
        when(channel.pipeline()).thenReturn(pipeline);
    }

    @Test
    void shouldInitiateSslHandshakeForLogin() {

        Prelogin prelogin = Prelogin.builder().build();

        ConnectionState.PRELOGIN.next(prelogin, nettyConnection);

        verify(pipeline).fireUserEventTriggered(SslState.LOGIN_ONLY);
    }

    @Test
    void shouldInitiateSslHandshakeForConnection() {

        Prelogin prelogin = Prelogin.builder().withEncryptionEnabled().build();

        ConnectionState.PRELOGIN.next(prelogin, nettyConnection);

        verify(pipeline).fireUserEventTriggered(SslState.CONNECTION);
    }

    @Test
    void shouldAdvanceToSslHandshakeState() {

        Prelogin prelogin = Prelogin.builder().withEncryptionEnabled().build();

        ConnectionState next = ConnectionState.PRELOGIN.next(prelogin, nettyConnection);

        assertThat(next).isEqualTo(ConnectionState.PRELOGIN_SSL_NEGOTIATION);
    }

    @Test
    void shouldAdvancePreloginState() {

        Prelogin prelogin = Prelogin.builder().withEncryptionNotSupported().build();

        ConnectionState next = ConnectionState.PRELOGIN.next(prelogin, nettyConnection);

        assertThat(next).isEqualTo(ConnectionState.PRELOGIN);
    }
}
