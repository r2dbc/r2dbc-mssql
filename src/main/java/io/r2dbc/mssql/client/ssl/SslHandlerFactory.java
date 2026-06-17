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

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.r2dbc.mssql.client.ClientConfiguration;

import java.security.GeneralSecurityException;
import java.util.function.Function;

/**
 * Factory for {@link SslHandler} instances configured from a {@link ClientConfiguration}.
 *
 * <p>{@link SslHandler}s produced by {@link #createSslHandler(ByteBufAllocator)} are configured with the connection's
 * peer host and port so the underlying engine can perform endpoint identification such as hostname verification and SNI.
 *
 * @author Mark Paluch
 * @see SslConfiguration
 * @see ClientConfiguration
 * @since 1.0.5
 */
public class SslHandlerFactory {

    private final ClientConfiguration configuration;

    private final SslConfiguration sslConfiguration;

    private SslHandlerFactory(ClientConfiguration configuration, SslConfiguration sslConfiguration) {
        this.configuration = configuration;
        this.sslConfiguration = sslConfiguration;
    }

    /**
     * Create a new {@code SslHandlerFactory} for the given configuration.
     *
     * @param configuration   the client configuration providing the peer address and SSL settings.
     * @param sslModeFunction selector resolving the {@link SslConfiguration} to apply from {@code configuration}.
     * @return a new {@code SslHandlerFactory}.
     */
    public static SslHandlerFactory create(ClientConfiguration configuration, Function<ClientConfiguration, SslConfiguration> sslModeFunction) {
        return new SslHandlerFactory(configuration, sslModeFunction.apply(configuration));
    }

    /**
     * Return whether SSL is enabled for the selected configuration.
     *
     * @return {@literal true} if SSL is enabled; {@literal false} otherwise.
     * @see SslConfiguration#isSslEnabled()
     */
    public boolean isSslEnabled() {
        return this.sslConfiguration.isSslEnabled();
    }

    /**
     * Create an {@link SslHandler} backed by a freshly created {@link SslContext SSL engine}.
     *
     * @param allocator the {@link ByteBufAllocator} the engine uses to allocate buffers.
     * @return a new {@link SslHandler}.
     * @throws GeneralSecurityException if setting up the SSL context or engine fails.
     */
    public SslHandler createSslHandler(ByteBufAllocator allocator)
            throws GeneralSecurityException {
        SslContext sslContext = this.sslConfiguration.getSslContext();
        return new SslHandler(sslContext.newEngine(allocator, this.configuration.getHost(), this.configuration.getPort()));
    }

}
