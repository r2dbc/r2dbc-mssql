/*
 * Copyright 2026 the original author or authors.
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

package io.r2dbc.mssql.client;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.r2dbc.mssql.client.ssl.SslConfiguration;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ReactorNettyClient}.
 */
class ReactorNettyClientUnitTests {

    @Test
    void createSslTunnelHandlerConfiguresPeerHostAndPort() throws Exception {

        SslHandler sslHandler = ReactorNettyClient.createSslTunnelHandler(TestByteBufAllocator.TEST, sslConfiguration(SslContextBuilder.forClient().build()), "localhost", 1433);

        assertThat(sslHandler.engine().getPeerHost()).isEqualTo("localhost");
        assertThat(sslHandler.engine().getPeerPort()).isEqualTo(1433);
    }

    private static SslConfiguration sslConfiguration(SslContext sslContext) {

        return new SslConfiguration() {

            @Override
            public boolean isSslEnabled() {
                return true;
            }

            @Override
            public SslContext getSslContext() {
                return sslContext;
            }
        };
    }
}
