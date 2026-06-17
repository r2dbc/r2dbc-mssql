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

package io.r2dbc.mssql.client.ssl;

import io.netty.handler.ssl.SslHandler;
import io.r2dbc.mssql.MssqlConnectionConfiguration;
import io.r2dbc.mssql.client.ClientConfiguration;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SslHandlerFactory}.
 *
 * @author Mark Paluch
 */
class SslHandlerFactoryUnitTests {

    @Test
    void createSniSslHandler() throws Exception {

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
                .host("foo").port(1234).enableSsl()
                .username("sa").password("sa")
                .build();
        ClientConfiguration clientConfiguration = configuration.toClientConfiguration();

        SslHandlerFactory sslHandlerFactory = SslHandlerFactory.create(clientConfiguration, it -> it);
        SslHandler sslHandler = sslHandlerFactory.createSslHandler(TestByteBufAllocator.TEST);

        assertThat(sslHandler.engine().getPeerHost()).isEqualTo("foo");
        assertThat(sslHandler.engine().getPeerPort()).isEqualTo(1234);
    }

}
