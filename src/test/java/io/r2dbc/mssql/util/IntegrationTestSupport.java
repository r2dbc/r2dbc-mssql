/*
 * Copyright 2018-2019 the original author or authors.
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

package io.r2dbc.mssql.util;

import io.r2dbc.mssql.MssqlConnection;
import io.r2dbc.mssql.MssqlConnectionFactory;
import io.r2dbc.mssql.MssqlConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.function.Predicate;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static io.r2dbc.spi.ConnectionFactoryOptions.builder;

/**
 * Support class for integration tests.
 *
 * @author Mark Paluch
 */
public abstract class IntegrationTestSupport {

    @RegisterExtension
    protected static final MsSqlServerExtension SERVER = new MsSqlServerExtension();

    protected static MssqlConnectionFactory connectionFactory;

    protected static MssqlConnection connection;

    @BeforeAll
    static void beforeAll() {

        Predicate<String> preferCursoredExecution = sql -> sql.contains("cursored");

        ConnectionFactoryOptions options = builder()
            .option(DRIVER, MssqlConnectionFactoryProvider.MSSQL_DRIVER)
            .option(HOST, SERVER.getHost())
            .option(PORT, SERVER.getPort())
            .option(PASSWORD, SERVER.getPassword())
            .option(USER, SERVER.getUsername())
            .option(MssqlConnectionFactoryProvider.PREFER_CURSORED_EXECUTION, preferCursoredExecution)
            .build();

        connectionFactory = (MssqlConnectionFactory) ConnectionFactories.get(options);
        connection = connectionFactory.create().block();
    }

    @AfterAll
    static void afterAll() {

        if (connection != null) {
            connection.close().subscribe();
        }
    }
}
