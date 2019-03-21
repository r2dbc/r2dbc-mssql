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
     * Driver option value.
     */
    public static final String MSSQL_DRIVER = "mssql";

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

        builder.database(connectionFactoryOptions.getValue(DATABASE));
        builder.host(connectionFactoryOptions.getRequiredValue(HOST));
        builder.password(connectionFactoryOptions.getRequiredValue(PASSWORD));
        builder.username(connectionFactoryOptions.getRequiredValue(USER));
        builder.applicationName(connectionFactoryOptions.getRequiredValue(USER));

        MssqlConnectionConfiguration configuration = builder.build();
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Creating MssqlConnectionFactory with configuration [%s] from options [%s]", configuration, connectionFactoryOptions));
        }
        return new MssqlConnectionFactory(configuration);
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {

        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        String driver = connectionFactoryOptions.getValue(DRIVER);
        if (driver == null || !driver.equals(MSSQL_DRIVER)) {
            return false;
        }

        if (!connectionFactoryOptions.hasOption(HOST)) {
            return false;
        }

        if (!connectionFactoryOptions.hasOption(PASSWORD)) {
            return false;
        }

        if (!connectionFactoryOptions.hasOption(USER)) {
            return false;
        }

        return true;
    }

    @Override
    public String getDriver() {
        return MSSQL_DRIVER;
    }
}
