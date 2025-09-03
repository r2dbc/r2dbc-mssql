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

package io.r2dbc.mssql;

import io.netty.handler.ssl.SslContextBuilder;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.io.File;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.LOCK_WAIT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.SSL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

/**
 * An implementation of {@link ConnectionFactoryProvider} for creating {@link MssqlConnectionFactory}s.
 *
 * @author Mark Paluch
 */
public final class MssqlConnectionFactoryProvider implements ConnectionFactoryProvider {

    private final Logger logger = Loggers.getLogger(this.getClass());

    /**
     * Application name.
     */
    public static final Option<String> APPLICATION_NAME = Option.valueOf("applicationName");

    /**
     * Connection Id
     */
    public static final Option<UUID> CONNECTION_ID = Option.valueOf("connectionId");

    /**
     * Expected Hostname in SSL certificate. Supports wildcards.
     */
    public static final Option<String> HOSTNAME_IN_CERTIFICATE = Option.valueOf("hostNameInCertificate");

    /**
     * Configure whether to prefer cursored execution on a statement-by-statement basis. Value can be {@link Boolean}, a {@link Predicate}, or a {@link Class class name}. The {@link Predicate}
     * accepts the SQL query string and returns a boolean flag indicating preference.
     * {@code true} prefers cursors, {@code false} prefers direct execution.
     */
    public static final Option<Object> PREFER_CURSORED_EXECUTION = Option.valueOf("preferCursoredExecution");

    /**
     * Configure whether to send character data as unicode (NVARCHAR, NCHAR, NTEXT) or whether to use the database encoding. Enabled by default.
     * If disabled, {@link CharSequence} data is sent using the database-specific collation such as ASCII/MBCS instead of Unicode.
     */
    public static final Option<Boolean> SEND_STRING_PARAMETERS_AS_UNICODE = Option.valueOf("sendStringParametersAsUnicode");

    /**
     * Customizer {@link Function} for {@link SslContextBuilder}.
     *
     * @since 0.8.3
     */
    public static final Option<Function<SslContextBuilder, SslContextBuilder>> SSL_CONTEXT_BUILDER_CUSTOMIZER = Option.valueOf("sslContextBuilderCustomizer");

    /**
     * Enable SSL tunnel usage to encrypt all traffic right from the connect phase by providing a customizer {@link Function}. This option is required when using a SSL tunnel (e.g. stunnel or other
     * SSL terminator) in front of the SQL server and it is not related to SQL Server's built-in SSL support.
     *
     * @since 0.8.5
     */
    public static final Option<Function<SslContextBuilder, SslContextBuilder>> SSL_TUNNEL = Option.valueOf("sslTunnel");

    /**
     * Enable/Disable TCP KeepAlive.
     *
     * @since 0.8.5
     */
    public static final Option<Boolean> TCP_KEEPALIVE = Option.valueOf("tcpKeepAlive");

    /**
     * Enable/Disable TCP NoDelay.
     *
     * @since 0.8.5
     */
    public static final Option<Boolean> TCP_NODELAY = Option.valueOf("tcpNoDelay");

    /**
     * Allow using SSL by fully trusting the server certificate. Enabling this option skips certificate verification.
     *
     * @since 0.8.6
     */
    public static final Option<Boolean> TRUST_SERVER_CERTIFICATE = Option.valueOf("trustServerCertificate");

    /**
     * Type of the TrustStore.
     *
     * @since 0.8.3
     */
    public static final Option<String> TRUST_STORE_TYPE = Option.valueOf("trustStoreType");

    /**
     * Path to the certificate TrustStore file.
     *
     * @since 0.8.3
     */
    public static final Option<File> TRUST_STORE = Option.valueOf("trustStore");

    /**
     * Password used to check the integrity of the TrustStore data.
     *
     * @since 0.8.3
     */
    public static final Option<char[]> TRUST_STORE_PASSWORD = Option.valueOf("trustStorePassword");

    /**
     * Optional {@link reactor.netty.resources.ConnectionProvider} to control Netty configuration directly
     *
     * @since 1.1.0
     */
    public static final Option<ConnectionProvider> CONNECTION_PROVIDER = Option.valueOf("connectionProvider");

    /**
     * Driver option value.
     */
    public static final String MSSQL_DRIVER = "sqlserver";

    /**
     * Driver option value.
     */
    public static final String ALTERNATE_MSSQL_DRIVER = "mssql";

    @SuppressWarnings("unchecked")
    @Override
    public MssqlConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {

        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        MssqlConnectionConfiguration.Builder builder = MssqlConnectionConfiguration.builder();

        OptionMapper mapper = OptionMapper.create(connectionFactoryOptions);

        mapper.fromTyped(APPLICATION_NAME).to(builder::applicationName);
        mapper.from(CONNECTION_ID).map(OptionMapper::toUuid).to(builder::connectionId);
        mapper.from(CONNECT_TIMEOUT).map(OptionMapper::toDuration).to(builder::connectTimeout);
        mapper.fromTyped(DATABASE).to(builder::database);
        mapper.fromTyped(HOSTNAME_IN_CERTIFICATE).to(builder::hostNameInCertificate);
        mapper.from(LOCK_WAIT_TIMEOUT).map(OptionMapper::toDuration).to(builder::lockWaitTimeout);
        mapper.from(PORT).map(OptionMapper::toInteger).to(builder::port);
        mapper.from(PREFER_CURSORED_EXECUTION).map(OptionMapper::toStringPredicate).to(builder::preferCursoredExecution);
        mapper.from(SEND_STRING_PARAMETERS_AS_UNICODE).map(OptionMapper::toBoolean).to(builder::sendStringParametersAsUnicode);
        mapper.from(SSL).map(OptionMapper::toBoolean).to(ssl -> {

            if (ssl) {
                builder.enableSsl();
            }
        });
        mapper.fromTyped(SSL_CONTEXT_BUILDER_CUSTOMIZER).to(builder::sslContextBuilderCustomizer);
        mapper.from(SSL_TUNNEL).map(it -> {

            if (it instanceof Boolean) {
                if ((Boolean) it) {
                    return Function.identity();
                }
                return null;
            }

            return it;

        }).to(it -> {

            if (it != null) {
                builder.enableSslTunnel((Function<SslContextBuilder, SslContextBuilder>) it);
            }
        });

        mapper.from(TCP_KEEPALIVE).map(OptionMapper::toBoolean).to(builder::tcpKeepAlive);
        mapper.from(TCP_NODELAY).map(OptionMapper::toBoolean).to(builder::tcpNoDelay);
        mapper.from(TRUST_SERVER_CERTIFICATE).map(OptionMapper::toBoolean).to((Consumer<Boolean>) builder::trustServerCertificate);
        mapper.from(TRUST_STORE).map(OptionMapper::toFile).to(builder::trustStore);
        mapper.fromTyped(TRUST_STORE_TYPE).to(builder::trustStoreType);
        mapper.from(TRUST_STORE_PASSWORD).map(it -> it instanceof String ? ((String) it).toCharArray() : (char[]) it).to(builder::trustStorePassword);
        mapper.fromTyped(CONNECTION_PROVIDER).to(builder::connectionProvider);

        builder.host(connectionFactoryOptions.getRequiredValue(HOST).toString());
        builder.password((CharSequence) connectionFactoryOptions.getRequiredValue(PASSWORD));
        builder.username(connectionFactoryOptions.getRequiredValue(USER).toString());

        MssqlConnectionConfiguration configuration = builder.build();
        if (this.logger.isDebugEnabled()) {
            this.logger.debug(String.format("Creating MssqlConnectionFactory with configuration [%s] from options [%s]", configuration, connectionFactoryOptions));
        }
        return new MssqlConnectionFactory(configuration);
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {

        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        Object driver = connectionFactoryOptions.getValue(DRIVER);
        if (driver == null || !(driver.equals(MSSQL_DRIVER) || driver.equals(ALTERNATE_MSSQL_DRIVER))) {
            return false;
        }

        if (!connectionFactoryOptions.hasOption(HOST)) {
            return false;
        }

        if (!connectionFactoryOptions.hasOption(PASSWORD)) {
            return false;
        }

        return connectionFactoryOptions.hasOption(USER);
    }

    @Override
    public String getDriver() {
        return MSSQL_DRIVER;
    }

}
