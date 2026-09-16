/*
 * Copyright 2019 the original author or authors.
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

import io.r2dbc.mssql.client.ClientConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.ALTERNATE_MSSQL_DRIVER;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.HOSTNAME_IN_CERTIFICATE;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.MSSQL_DRIVER;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.PREFER_CURSORED_EXECUTION;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.SEND_STRING_PARAMETERS_AS_UNICODE;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.SSL_CONTEXT_BUILDER_CUSTOMIZER;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.SSL_TUNNEL;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.TCP_KEEPALIVE;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.TCP_NODELAY;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.TRUST_SERVER_CERTIFICATE;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.TRUST_STORE;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.TRUST_STORE_PASSWORD;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.TRUST_STORE_TYPE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.LOCK_WAIT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.SSL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static io.r2dbc.spi.ConnectionFactoryOptions.builder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Unit tests for {@link MssqlConnectionFactoryProvider}.
 *
 * @author Mark Paluch
 */
final class MssqlConnectionFactoryProviderUnitTests {

    private final MssqlConnectionFactoryProvider provider = new MssqlConnectionFactoryProvider();

    @Test
    void supportsMssqlDriver() {

        assertThat(this.provider.getDriver()).isEqualTo(MSSQL_DRIVER);
        assertThat(this.provider.supports(options().build())).isTrue();
        assertThat(this.provider.supports(options().option(DRIVER, ALTERNATE_MSSQL_DRIVER).build())).isTrue();
        assertThat(this.provider.supports(options().option(DRIVER, "test-driver").build())).isFalse();
    }

    @Test
    void requiresDriverHostUserAndPassword() {

        assertThat(this.provider.supports(builder().option(HOST, "test-host").option(USER, "test-user").option(PASSWORD, "test-password").build())).isFalse();
        assertThat(this.provider.supports(builder().option(DRIVER, MSSQL_DRIVER).option(USER, "test-user").option(PASSWORD, "test-password").build())).isFalse();
        assertThat(this.provider.supports(builder().option(DRIVER, MSSQL_DRIVER).option(HOST, "test-host").option(PASSWORD, "test-password").build())).isFalse();
        assertThat(this.provider.supports(builder().option(DRIVER, MSSQL_DRIVER).option(HOST, "test-host").option(USER, "test-user").build())).isFalse();
    }

    @Test
    void createsConfigurationFromUrl() {

        MssqlConnectionFactory factory = (MssqlConnectionFactory) ConnectionFactories.get("r2dbc:mssql://test-user:test-password@localhost:15433/testdb?serverName=sql.example.com");

        MssqlConnectionConfiguration configuration = factory.getConfiguration();

        assertThat(configuration.getHost()).isEqualTo("localhost");
        assertThat(configuration.getPort()).isEqualTo(15433);
        assertThat(configuration.getDatabase()).contains("testdb");
        assertThat(configuration.getUsername()).isEqualTo("test-user");
        assertThat(configuration.getServerName()).isEqualTo("sql.example.com");
        assertThat(configuration.getHostNameInCertificate()).isEqualTo("sql.example.com");
    }

    @Test
    void mapsLockWaitTimeout() {
        assertThat(create(options -> options.option(LOCK_WAIT_TIMEOUT, Duration.ofSeconds(10))).getConfiguration().getLockWaitTimeout()).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void mapsCursoredExecutionPreference() {

        assertThat(create(options -> options.option(PREFER_CURSORED_EXECUTION, true)).getConnectionOptions().prefersCursors("foo")).isTrue();
        assertThat(create(options -> options.option(PREFER_CURSORED_EXECUTION, "true")).getConnectionOptions().prefersCursors("foo")).isTrue();

        ConnectionOptions byClassName = create(options -> options.option(PREFER_CURSORED_EXECUTION, MyPredicate.class.getName())).getConnectionOptions();

        assertThat(byClassName.prefersCursors("foo")).isTrue();
        assertThat(byClassName.prefersCursors("bar")).isFalse();
    }

    @Test
    void mapsSendStringParametersAsUnicode() {

        assertThat(create().getConnectionOptions().isSendStringParametersAsUnicode()).isTrue();
        assertThat(create(options -> options.option(SEND_STRING_PARAMETERS_AS_UNICODE, false)).getConnectionOptions().isSendStringParametersAsUnicode()).isFalse();
    }

    @Test
    void mapsTcpOptions() {

        ClientConfiguration defaults = create().getClientConfiguration();

        assertThat(defaults.isTcpKeepAlive()).isFalse();
        assertThat(defaults.isTcpNoDelay()).isTrue();

        ClientConfiguration configured = create(options -> options.option(TCP_KEEPALIVE, true).option(TCP_NODELAY, false)).getClientConfiguration();

        assertThat(configured.isTcpKeepAlive()).isTrue();
        assertThat(configured.isTcpNoDelay()).isFalse();
    }

    @Test
    void mapsSsl() {

        assertThat(create().getClientConfiguration().isSslEnabled()).isFalse();

        MssqlConnectionFactory factory = create(options -> options.option(SSL, true).option(HOSTNAME_IN_CERTIFICATE, "*.foo"));

        assertThat(factory.getClientConfiguration().isSslEnabled()).isTrue();
        assertThat(factory.getClientConfiguration().getSslTunnelConfiguration().isSslEnabled()).isFalse();
        assertThat(factory.getConfiguration().getHostNameInCertificate()).isEqualTo("*.foo");
    }

    @Test
    void mapsSslContextBuilderCustomizer() {

        MssqlConnectionFactory factory = create(options -> options.option(SSL, true).option(SSL_CONTEXT_BUILDER_CUSTOMIZER, sslContextBuilder -> {
            throw new IllegalStateException("Works!");
        }));

        assertThatIllegalStateException().isThrownBy(() -> factory.getClientConfiguration().getSslContext()).withMessage("Works!");
    }

    @Test
    void mapsSslTunnel() {

        assertThat(create(options -> options.option(Option.valueOf("sslTunnel"), true)).getClientConfiguration().getSslTunnelConfiguration().isSslEnabled()).isTrue();
        assertThat(create(options -> options.option(Option.valueOf("sslTunnel"), false)).getClientConfiguration().getSslTunnelConfiguration().isSslEnabled()).isFalse();
        assertThat(create(options -> options.option(SSL_TUNNEL, Function.identity())).getClientConfiguration().getSslTunnelConfiguration().isSslEnabled()).isTrue();
    }

    @Test
    void mapsTrustSettings() {

        assertThat(create(options -> options.option(TRUST_SERVER_CERTIFICATE, true)).getClientConfiguration()).extracting("trustServerCertificate").isEqualTo(true);

        ClientConfiguration configuration = create(options -> options
            .option(TRUST_STORE, new File("foo"))
            .option(TRUST_STORE_PASSWORD, "hello".toCharArray())
            .option(TRUST_STORE_TYPE, "PKCS")).getClientConfiguration();

        assertThat(configuration).extracting("trustServerCertificate", "trustStore", "trustStoreType").containsExactly(false, new File("foo"), "PKCS");
        assertThat(configuration).extracting("trustStorePassword").isEqualTo("hello".toCharArray());
    }

    private MssqlConnectionFactory create() {
        return create(UnaryOperator.identity());
    }

    private MssqlConnectionFactory create(UnaryOperator<ConnectionFactoryOptions.Builder> customizer) {
        return this.provider.create(customizer.apply(options()).build());
    }

    private static ConnectionFactoryOptions.Builder options() {
        return builder().option(DRIVER, MSSQL_DRIVER).option(HOST, "test-host").option(USER, "test-user").option(PASSWORD, "test-password");
    }

    static class MyPredicate implements Predicate<String> {

        @Override
        public boolean test(String s) {
            return s.equals("foo");
        }

    }

}
