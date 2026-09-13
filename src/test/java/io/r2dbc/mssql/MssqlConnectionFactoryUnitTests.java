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

import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.Redirect;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.message.token.*;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link MssqlConnectionFactory}.
 *
 * @author Mark Paluch
 */
class MssqlConnectionFactoryUnitTests {

    MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder().host("initial").username("user").password("password").build();

    static final Column[] COLUMNS = Arrays.asList(
        createColumn(0, "Edition", SqlServerType.NVARCHAR, 100, LengthStrategy.USHORTLENTYPE, ServerCharset.UNICODE.charset()),
        createColumn(1, "VersionString", SqlServerType.VARCHAR, 100, LengthStrategy.USHORTLENTYPE, ServerCharset.CP1252.charset())).toArray(new Column[0]);

    @Test
    void constructorNoClientFactory() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MssqlConnectionFactory(null, MssqlConnectionConfiguration.builder()
            .host("test-host")
            .password("test-password")
            .username("test-username")
            .build()))
            .withMessage("clientFactory must not be null");
    }

    @Test
    void constructorNoConfiguration() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MssqlConnectionFactory(null))
            .withMessage("configuration must not be null");
    }

    @Test
    void constructorNoInstancePortResolver() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MssqlConnectionFactory(config -> Mono.empty(), null, this.configuration))
            .withMessage("instancePortResolver must not be null");
    }

    @Test
    void shouldResolveNamedInstanceWhenPortIsNotExplicitlyConfigured() {

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .host("sql01")
            .instanceName("SQLEXPRESS")
            .username("user")
            .password("password")
            .build();

        AtomicReference<String> resolvedHost = new AtomicReference<>();
        AtomicReference<String> resolvedInstance = new AtomicReference<>();

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> Mono.empty(), (host, instanceName) -> {
            resolvedHost.set(host);
            resolvedInstance.set(instanceName);
            return Mono.just(51432);
        }, configuration);

        StepVerifier.create(connectionFactory.resolveConfiguration(configuration))
            .assertNext(resolved -> {
                assertThat(resolved.getHost()).isEqualTo("sql01");
                assertThat(resolved.getInstanceName()).contains("SQLEXPRESS");
                assertThat(resolved.getPort()).isEqualTo(51432);
                assertThat(resolved.isPortConfigured()).isTrue();
            })
            .verifyComplete();

        assertThat(resolvedHost.get()).isEqualTo("sql01");
        assertThat(resolvedInstance.get()).isEqualTo("SQLEXPRESS");
    }

    @Test
    void shouldSkipNamedInstanceResolutionWithoutInstanceName() {

        AtomicBoolean resolverCalled = new AtomicBoolean();
        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> Mono.empty(), (host, instanceName) -> {
            resolverCalled.set(true);
            return Mono.just(51432);
        }, this.configuration);

        StepVerifier.create(connectionFactory.resolveConfiguration(this.configuration))
            .expectNext(this.configuration)
            .verifyComplete();

        assertThat(resolverCalled.get()).isFalse();
    }

    @Test
    void shouldSkipNamedInstanceResolutionWhenPortIsExplicitlyConfigured() {

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .host("sql01")
            .instanceName("SQLEXPRESS")
            .port(1433)
            .username("user")
            .password("password")
            .build();

        AtomicBoolean resolverCalled = new AtomicBoolean();
        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> Mono.empty(), (host, instanceName) -> {
            resolverCalled.set(true);
            return Mono.just(51432);
        }, configuration);

        StepVerifier.create(connectionFactory.resolveConfiguration(configuration))
            .expectNext(configuration)
            .verifyComplete();

        assertThat(resolverCalled.get()).isFalse();
        assertThat(configuration.getPort()).isEqualTo(1433);
        assertThat(configuration.isPortConfigured()).isTrue();
    }

    @Test
    void shouldPropagateNamedInstanceResolutionFailure() {

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .host("sql01")
            .instanceName("SQLEXPRESS")
            .username("user")
            .password("password")
            .build();

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> Mono.empty(),
            (host, instanceName) -> Mono.error(new IllegalStateException("browser failure")), configuration);

        StepVerifier.create(connectionFactory.resolveConfiguration(configuration))
            .expectErrorMatches(error -> error instanceof IllegalStateException && error.getMessage().equals("browser failure"))
            .verify();
    }

    @Test
    void shouldConnectUsingResolvedNamedInstancePort() {

        ColumnMetadataToken columns = ColumnMetadataToken.create(COLUMNS);
        RowToken rowToken = RowTokenFactory.create(columns, buffer -> {
            Encode.uString(buffer, "Edition", ServerCharset.UNICODE.charset());
            Encode.uString(buffer, "1.2.3", ServerCharset.CP1252.charset());
        });

        TestClient client = TestClient.builder().assertNextRequestWith(clientMessage -> {
            assertThat(clientMessage).isInstanceOf(Prelogin.class);
        }).thenRespond(DoneToken.create(0)).assertNextRequestWith(clientMessage -> {
            assertThat(clientMessage).isInstanceOf(SqlBatch.class);
        }).thenRespond(columns, rowToken, DoneToken.create(1)).build();

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .host("sql01")
            .instanceName("SQLEXPRESS")
            .username("user")
            .password("password")
            .build();

        AtomicReference<MssqlConnectionConfiguration> connectedConfiguration = new AtomicReference<>();

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> {
            connectedConfiguration.set(config);
            return Mono.just(client);
        }, (host, instanceName) -> Mono.just(51432), configuration);

        connectionFactory.create().as(StepVerifier::create).expectNextCount(1).verifyComplete();

        assertThat(connectedConfiguration.get()).isNotNull();
        assertThat(connectedConfiguration.get().getHost()).isEqualTo("sql01");
        assertThat(connectedConfiguration.get().getPort()).isEqualTo(51432);
        assertThat(connectedConfiguration.get().isPortConfigured()).isTrue();
    }
    @Test
    void shouldFollowRedirect() {

        ColumnMetadataToken columns = ColumnMetadataToken.create(COLUMNS);
        RowToken rowToken = RowTokenFactory.create(columns, buffer -> {
            Encode.uString(buffer, "Edition", ServerCharset.UNICODE.charset());
            Encode.uString(buffer, "1.2.3", ServerCharset.CP1252.charset());
        });

        TestClient initial =
            TestClient.builder().expectClose().withRedirect(Redirect.create("redirect", 1234)).assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(Prelogin.class);

            }).thenRespond(DoneToken.create(0)).build();


        TestClient redirect =
            TestClient.builder().assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(Prelogin.class);

            }).thenRespond(DoneToken.create(0)).assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(SqlBatch.class);
            }).thenRespond(columns, rowToken, DoneToken.create(1)).build();

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> {

            if (config.getHost().equals("initial")) {
                return Mono.just(initial);
            }

            return Mono.just(redirect);
        }, this.configuration);


        connectionFactory.create().as(StepVerifier::create).expectNextCount(1).verifyComplete();

        assertThat(initial.isClosed()).isTrue();
        assertThat(redirect.isClosed()).isFalse();
    }

    @Test
    void properlyPropagatesFailures() {

        ErrorToken error = new ErrorToken(0, 0, (byte) 0, (byte) 0, "failure", "", "", 0);

        TestClient initial =
            TestClient.builder().expectClose().withRedirect(Redirect.create("redirect", 1234)).assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(Prelogin.class);

            }).thenRespond(error).build();

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> {
            return Mono.just(initial);
        }, this.configuration);


        connectionFactory.create().as(StepVerifier::create).verifyError(R2dbcNonTransientResourceException.class);

        assertThat(initial.isClosed()).isTrue();
    }

    @Test
    void shouldFailOnMultipleRedirects() {

        TestClient initial =
            TestClient.builder().expectClose().withRedirect(Redirect.create("redirect", 1234)).assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(Prelogin.class);

            }).thenRespond(DoneToken.create(0)).build();


        TestClient redirect =
            TestClient.builder().expectClose().withRedirect(Redirect.create("redirect", 1234)).assertNextRequestWith(clientMessage -> {

                assertThat(clientMessage).isInstanceOf(Prelogin.class);

            }).thenRespond(DoneToken.create(0)).build();

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> {

            if (config.getHost().equals("initial")) {
                return Mono.just(initial);
            }

            return Mono.just(redirect);
        }, this.configuration);


        connectionFactory.create().as(StepVerifier::create).verifyError(MssqlConnectionFactory.MssqlRoutingException.class);

        assertThat(initial.isClosed()).isTrue();
        assertThat(redirect.isClosed()).isTrue();
    }

    @Test
    void shouldCreateNewPreparedStatement() {

        MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(config -> Mono.empty(), this.configuration);
        ConnectionOptions options = connectionFactory.getConnectionOptions();
        ConnectionOptions other = connectionFactory.getConnectionOptions();

        options.getPreparedStatementCache().putHandle(1, "foo", new Binding());

        assertThat(options.getPreparedStatementCache().getHandle("foo", new Binding())).isEqualTo(1);
        assertThat(other.getPreparedStatementCache().getHandle("foo", new Binding())).isEqualTo(0);
    }

    private static Column createColumn(int index, String name, SqlServerType serverType, int length, LengthStrategy lengthStrategy, @Nullable Charset charset) {

        TypeInformation.Builder builder = TypeInformation.builder().withServerType(serverType).withMaxLength(length).withLengthStrategy(lengthStrategy);
        if (charset != null) {
            builder.withCharset(charset);
        }
        TypeInformation type = builder.build();

        return new Column(index, name, type, null);
    }
}
