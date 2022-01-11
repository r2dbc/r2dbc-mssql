/*
 * Copyright 2019-2022 the original author or authors.
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

import io.r2dbc.mssql.client.ssl.SslConfiguration;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

/**
 * Connection configuration details.
 *
 * @author Mark Paluch
 */
public interface ClientConfiguration extends SslConfiguration {

    /**
     * @return server hostname.
     */
    String getHost();

    /**
     * @return server port.
     */
    int getPort();

    /**
     * @return connection timeout.
     */
    Duration getConnectTimeout();

    /**
     * @return whether TCP KeepAlive is enabled.
     * @since 0.8.5
     */
    boolean isTcpKeepAlive();

    /**
     * @return whether TCP NoDelay is enabled.
     * @since 0.8.5
     */
    boolean isTcpNoDelay();

    /**
     * @return connection provider.
     */
    ConnectionProvider getConnectionProvider();

    /**
     * @return the SSL tunnel configuration.
     * @since 0.8.5
     */
    default SslConfiguration getSslTunnelConfiguration() {
        return DisabledSslTunnel.INSTANCE;
    }

}
