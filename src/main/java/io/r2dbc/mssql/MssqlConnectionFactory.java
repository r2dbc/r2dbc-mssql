/*
 * Copyright 2018-2022 the original author or authors.
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
import io.r2dbc.mssql.client.ClientConfiguration;
import io.r2dbc.mssql.client.ReactorNettyClient;
import io.r2dbc.mssql.message.tds.Redirect;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a Microsoft SQL Server database.
 *
 * @author Mark Paluch
 * @author Lars Haatveit
 */
public final class MssqlConnectionFactory implements ConnectionFactory {

    private final String METADATA_QUERY = " SELECT " +
        "CAST(SERVERPROPERTY('Edition') AS VARCHAR(255)) AS Edition, " +
        "CAST(@@VERSION AS VARCHAR(255)) as VersionString";

    private final Function<MssqlConnectionConfiguration, Mono<Client>> clientFactory;

    private final MssqlConnectionConfiguration configuration;

    private final ConnectionOptions connectionOptions;

    /**
     * Creates a new connection factory.
     *
     * @param configuration the configuration to use connections
     * @throws IllegalArgumentException when {@link MssqlConnectionConfiguration} is {@code null}.
     */
    public MssqlConnectionFactory(MssqlConnectionConfiguration configuration) {
        this(MssqlConnectionFactory::connect, configuration);
    }

    MssqlConnectionFactory(Function<MssqlConnectionConfiguration, Mono<Client>> clientFactory,
                           MssqlConnectionConfiguration configuration) {

        this.clientFactory = Assert.requireNonNull(clientFactory, "clientFactory must not be null");
        this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");
        this.connectionOptions = configuration.toConnectionOptions();
    }

    private static Mono<Client> connect(MssqlConnectionConfiguration configuration) {

        return Mono.defer(() -> {
            Assert.requireNonNull(configuration, "configuration must not be null");

            return ReactorNettyClient.connect(configuration.toClientConfiguration(), configuration.getApplicationName(),
                configuration.getConnectionId());
        });
    }

    private Mono<Client> initializeClient(MssqlConnectionConfiguration configuration, boolean allowReroute) {

        LoginConfiguration loginConfiguration = configuration.getLoginConfiguration();

        return this.clientFactory.apply(configuration)
            .delayUntil(client -> LoginFlow.exchange(client, loginConfiguration)
                .onErrorResume(e -> propagateError(client.close(), e)))
            .flatMap(client -> {
                return client.getRedirect().map(redirect -> {
                    if (allowReroute) {
                        return redirectClient(client, redirect);
                    } else {
                        return this.<Client>propagateError(client.close(), new MssqlRoutingException("Client was redirected more than once"));
                    }
                }).orElse(Mono.just(client));
            });
    }

    private Mono<Client> redirectClient(Client client, Redirect redirect) {

        MssqlConnectionConfiguration routeConfiguration = this.configuration.withRedirect(redirect);

        return client.close().then(this.initializeClient(routeConfiguration, false));
    }

    private <T> Mono<T> propagateError(Mono<?> action, Throwable e) {

        return action.onErrorResume(suppressed -> {
            e.addSuppressed(suppressed);
            return Mono.error(e);
        }).then(Mono.error(e));
    }

    @Override
    public Mono<MssqlConnection> create() {

        return initializeClient(this.configuration, true)
            .flatMap(it -> {

                Flux<MssqlConnection> connectionFlux =
                    new SimpleMssqlStatement(it, this.connectionOptions, this.METADATA_QUERY).execute()
                        .flatMap(result -> result.map((row, rowMetadata) -> toConnectionMetadata(it.getDatabaseVersion().orElse("unknown"), row))).map(metadata -> {
                        return new MssqlConnection(it, metadata, this.connectionOptions);
                    });

                return connectionFlux.last().onErrorResume(throwable -> {
                    return it.close().then(Mono.error(new R2dbcNonTransientResourceException("Cannot connect to " + this.configuration.getHost() + ":" + this.configuration.getPort(), throwable)));
                });
            });
    }

    private static MssqlConnectionMetadata toConnectionMetadata(String version, Row row) {
        return MssqlConnectionMetadata.from(row.get("Edition", String.class), version, row.get("VersionString", String.class));
    }

    ClientConfiguration getClientConfiguration() {
        return this.configuration.toClientConfiguration();
    }

    ConnectionOptions getConnectionOptions() {
        return this.connectionOptions;
    }

    @Override
    public MssqlConnectionFactoryMetadata getMetadata() {
        return MssqlConnectionFactoryMetadata.INSTANCE;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [configuration=").append(this.configuration);
        sb.append(']');
        return sb.toString();
    }

    static class MssqlRoutingException extends R2dbcNonTransientResourceException {

        public MssqlRoutingException(String reason) {
            super(reason);
        }

    }

}
