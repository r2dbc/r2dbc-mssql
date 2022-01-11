/*
 * Copyright 2019-2021 the original author or authors.
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
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.ALTERNATE_MSSQL_DRIVER;
import static io.r2dbc.mssql.MssqlConnectionFactoryProvider.MSSQL_DRIVER;
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
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
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
final class MssqlConnectionFactoryProviderTest {

    private final MssqlConnectionFactoryProvider provider = new MssqlConnectionFactoryProvider();

    @Test
    void doesNotSupportWithWrongDriver() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, "test-driver")
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(PORT, -1)
            .option(USER, "test-user")
            .build())).isFalse();
    }

    @Test
    void doesNotSupportWithoutDriver() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(PORT, -1)
            .option(USER, "test-user")
            .build())).isFalse();
    }

    @Test
    void doesNotSupportWithoutHost() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, MSSQL_DRIVER)
            .option(PASSWORD, "test-password")
            .option(PORT, -1)
            .option(USER, "test-user")
            .build())).isFalse();
    }

    @Test
    void doesNotSupportWithoutPassword() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PORT, -1)
            .option(USER, "test-user")
            .build())).isFalse();
    }

    @Test
    void doesNotSupportWithoutUser() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(PORT, -1)
            .build())).isFalse();
    }

    @Test
    void supports() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .build())).isTrue();
    }

    @Test
    void supportsAlternateDriverId() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, ALTERNATE_MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .build())).isTrue();
    }

    @Test
    void shouldConfigureWithStaticCursoredExecutionPreference() {

        MssqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(MssqlConnectionFactoryProvider.PREFER_CURSORED_EXECUTION, "true")
            .build());

        ConnectionOptions options = factory.getConnectionOptions();

        assertThat(options.prefersCursors("foo")).isTrue();
    }

    @Test
    void shouldConfigureWithStringAsUnicode() {

        MssqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(SSL, true)
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(MssqlConnectionFactoryProvider.SEND_STRING_PARAMETERS_AS_UNICODE, false)
            .build());

        assertThat(factory.getConnectionOptions().isSendStringParametersAsUnicode()).isFalse();

        factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(SSL, true)
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(MssqlConnectionFactoryProvider.SEND_STRING_PARAMETERS_AS_UNICODE, true)
            .build());

        assertThat(factory.getConnectionOptions().isSendStringParametersAsUnicode()).isTrue();
    }

    @Test
    void shouldConfigureWithSsl() {

        MssqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(SSL, true)
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(MssqlConnectionFactoryProvider.HOSTNAME_IN_CERTIFICATE, "*.foo")
            .build());

        ClientConfiguration configuration = factory.getClientConfiguration();

        assertThat(configuration.isSslEnabled()).isTrue();
        assertThat(configuration.getSslTunnelConfiguration().isSslEnabled()).isFalse();
    }

    @Test
    void shouldConfigureWithoutSsl() {

        MssqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(SSL, false)
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .build());

        ClientConfiguration configuration = factory.getClientConfiguration();

        assertThat(configuration.isSslEnabled()).isFalse();
    }

    @Test
    void shouldConfigureWithSslCustomizer() {

        MssqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(SSL, true)
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(SSL_CONTEXT_BUILDER_CUSTOMIZER, sslContextBuilder -> {
                throw new IllegalStateException("Works!");
            })
            .build());

        assertThatIllegalStateException().isThrownBy(() -> factory.getClientConfiguration().getSslProvider()).withMessageContaining("Works!");
    }

    @Test
    void shouldConfigureWithSslTunnel() {

        MssqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(SSL, true)
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(Option.valueOf("sslTunnel"), true)
            .build());

        assertThat(factory.getClientConfiguration().getSslTunnelConfiguration().isSslEnabled()).isTrue();

        factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(SSL, true)
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(Option.valueOf("sslTunnel"), false)
            .build());

        assertThat(factory.getClientConfiguration().getSslTunnelConfiguration().isSslEnabled()).isFalse();
    }

    @Test
    void shouldConfigureWithSslTunnelCustomizer() {

        MssqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(SSL, true)
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(SSL_TUNNEL, Function.identity())
            .build());

        assertThat(factory.getClientConfiguration().getSslTunnelConfiguration().isSslEnabled()).isTrue();
    }

    @Test
    void shouldConfigureTcpKeepAlive() {

        MssqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(TCP_KEEPALIVE, true)
            .build());

        assertThat(factory.getClientConfiguration().isTcpKeepAlive()).isTrue();
    }

    @Test
    void shouldConfigureTcpNoDelay() {

        MssqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(TCP_NODELAY, true)
            .build());

        assertThat(factory.getClientConfiguration().isTcpNoDelay()).isTrue();
    }

    @Test
    void shouldConfigureWithTrustServerCertificate() {

        MssqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(SSL, true)
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(TRUST_SERVER_CERTIFICATE, true)
            .option(TRUST_STORE_PASSWORD, "hello".toCharArray())
            .option(TRUST_STORE_TYPE, "PKCS")
            .build());

        assertThat(factory.getClientConfiguration())
            .hasFieldOrPropertyWithValue("trustServerCertificate", true);
    }

    @Test
    void shouldConfigureWithTrustStoreCustomizer() {

        MssqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(SSL, true)
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(TRUST_STORE, new File("foo"))
            .option(TRUST_STORE_PASSWORD, "hello".toCharArray())
            .option(TRUST_STORE_TYPE, "PKCS")
            .build());

        assertThat(factory.getClientConfiguration())
            .hasFieldOrPropertyWithValue("trustServerCertificate", false)
            .hasFieldOrPropertyWithValue("trustStore", new File("foo"))
            .hasFieldOrPropertyWithValue("trustStorePassword", "hello".toCharArray())
            .hasFieldOrPropertyWithValue("trustStoreType", "PKCS");
    }

    @Test
    void shouldConfigureWithStaticBooleanCursoredExecutionPreference() {

        MssqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(MssqlConnectionFactoryProvider.PREFER_CURSORED_EXECUTION, true)
            .build());

        ConnectionOptions options = factory.getConnectionOptions();

        assertThat(options.prefersCursors("foo")).isTrue();
    }

    @Test
    void shouldConfigureWithClassCursoredExecutionPreference() {

        MssqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.builder()
            .option(DRIVER, MSSQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(MssqlConnectionFactoryProvider.PREFER_CURSORED_EXECUTION, MyPredicate.class.getName())
            .build());

        ConnectionOptions options = factory.getConnectionOptions();

        assertThat(options.prefersCursors("foo")).isTrue();
        assertThat(options.prefersCursors("bar")).isFalse();
    }

    @Test
    void returnsDriverIdentifier() {
        assertThat(this.provider.getDriver()).isEqualTo(MSSQL_DRIVER);
    }

    static class MyPredicate implements Predicate<String> {

        @Override
        public boolean test(String s) {
            return s.equals("foo");
        }

    }

}
