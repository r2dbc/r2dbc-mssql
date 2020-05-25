/*
 * Copyright 2019-2020 the original author or authors.
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
import reactor.util.Logger;
import reactor.util.Loggers;

import java.io.File;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
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

        mapper.from(APPLICATION_NAME).to(builder::applicationName);
        mapper.from(CONNECTION_ID).map(OptionMapper::toUuid).to(builder::connectionId);
        mapper.from(CONNECT_TIMEOUT).map(OptionMapper::toDuration).to(builder::connectTimeout);
        mapper.from(DATABASE).to(builder::database);
        mapper.from(HOSTNAME_IN_CERTIFICATE).to(builder::hostNameInCertificate);
        mapper.from(PORT).map(OptionMapper::toInteger).to(builder::port);
        mapper.from(PREFER_CURSORED_EXECUTION).map(OptionMapper::toStringPredicate).to(builder::preferCursoredExecution);
        mapper.from(SEND_STRING_PARAMETERS_AS_UNICODE).map(OptionMapper::toBoolean).to(builder::sendStringParametersAsUnicode);
        mapper.from(SSL).to(builder::enableSsl);
        mapper.from(SSL_CONTEXT_BUILDER_CUSTOMIZER).to(builder::sslContextBuilderCustomizer);
        mapper.from(TRUST_STORE).map(OptionMapper::toFile).to(builder::trustStore);
        mapper.from(TRUST_STORE_TYPE).to(builder::trustStoreType);
        mapper.from(TRUST_STORE_PASSWORD).map(it -> it instanceof String ? ((String) it).toCharArray() : (char[]) it).to(builder::trustStorePassword);

        builder.host(connectionFactoryOptions.getRequiredValue(HOST));
        builder.password(connectionFactoryOptions.getRequiredValue(PASSWORD));
        builder.username(connectionFactoryOptions.getRequiredValue(USER));

        MssqlConnectionConfiguration configuration = builder.build();
        if (this.logger.isDebugEnabled()) {
            this.logger.debug(String.format("Creating MssqlConnectionFactory with configuration [%s] from options [%s]", configuration, connectionFactoryOptions));
        }
        return new MssqlConnectionFactory(configuration);
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {

        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        String driver = connectionFactoryOptions.getValue(DRIVER);
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
