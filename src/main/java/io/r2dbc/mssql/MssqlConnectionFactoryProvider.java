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

import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.UUID;
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

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

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

        String applicationName = connectionFactoryOptions.getValue(APPLICATION_NAME);
        if (applicationName != null) {
            builder.applicationName(applicationName);
        }

        Boolean ssl = connectionFactoryOptions.getValue(SSL);
        if (ssl != null && ssl) {
            builder.enableSsl();
        }

        String hostNameInCertificate = connectionFactoryOptions.getValue(HOSTNAME_IN_CERTIFICATE);

        if (hostNameInCertificate != null) {
            builder.hostNameInCertificate(hostNameInCertificate);
        }

        Integer port = connectionFactoryOptions.getValue(PORT);
        if (port != null) {
            builder.port(port);
        }

        UUID connectionId = connectionFactoryOptions.getValue(CONNECTION_ID);
        if (connectionId != null) {
            builder.connectionId(connectionId);
        }

        Duration connectTimeout = connectionFactoryOptions.getValue(CONNECT_TIMEOUT);
        if (connectTimeout != null) {
            builder.connectTimeout(connectTimeout);
        }

        Object preferCursoredExecution = connectionFactoryOptions.getValue(PREFER_CURSORED_EXECUTION);

        if (preferCursoredExecution instanceof Predicate) {
            builder.preferCursoredExecution((Predicate<String>) preferCursoredExecution);
        }

        if (preferCursoredExecution instanceof Boolean) {
            builder.preferCursoredExecution((boolean) preferCursoredExecution);
        }

        if (preferCursoredExecution instanceof String) {

            String value = (String) preferCursoredExecution;
            if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                builder.preferCursoredExecution((Boolean.valueOf(value)));
            } else {

                try {
                    Object predicate = Class.forName(value).newInstance();
                    if (predicate instanceof Predicate) {
                        builder.preferCursoredExecution((Predicate<String>) predicate);
                    } else {
                        throw new IllegalArgumentException("Value '" + value + "' must be an instance of Predicate");
                    }
                } catch (ReflectiveOperationException e) {
                    throw new IllegalArgumentException("Cannot instantiate '" + value + "'", e);
                }
            }
        }

        builder.database(connectionFactoryOptions.getValue(DATABASE));
        builder.host(connectionFactoryOptions.getRequiredValue(HOST));
        builder.password(connectionFactoryOptions.getRequiredValue(PASSWORD));
        builder.username(connectionFactoryOptions.getRequiredValue(USER));
        builder.applicationName(connectionFactoryOptions.getRequiredValue(USER));

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
