/*
 * Copyright 2018-2021 the original author or authors.
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

import com.zaxxer.hikari.HikariDataSource;
import io.r2dbc.mssql.MssqlConnectionConfiguration;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.net.Socket;
import java.util.function.Supplier;

/**
 * Test container extension for Microsoft SQL Server.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public final class MsSqlServerExtension implements BeforeAllCallback, AfterAllCallback {

    private volatile MSSQLServerContainer<?> containerInstance = null;

    private final Supplier<MSSQLServerContainer<?>> container = () -> {

        if (this.containerInstance != null) {
            return this.containerInstance;
        }
        return this.containerInstance = new MSSQLServerContainer() {

            protected void configure() {
                this.addExposedPort(MS_SQL_SERVER_PORT);
                this.addEnv("ACCEPT_EULA", "Y");
                this.addEnv("SA_PASSWORD", getPassword());
                this.withReuse(true);
            }
        };
    };

    private HikariDataSource dataSource;

    private JdbcOperations jdbcOperations;

    private final DatabaseContainer sqlServer = External.INSTANCE.isAvailable() ? External.INSTANCE : new TestContainer(this.container.get());

    private final boolean useTestContainer = this.sqlServer instanceof TestContainer;

    @Override
    public void beforeAll(ExtensionContext context) {
        initialize();
    }

    public void initialize() {

        if (this.useTestContainer) {
            this.container.get().start();
        }

        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setJdbcUrl("jdbc:sqlserver://" + getHost() + ":" + getPort() + ";database=master;sendStringParametersAsUnicode=true");
        hikariDataSource.setUsername(getUsername());
        hikariDataSource.setPassword(getPassword());

        this.dataSource = hikariDataSource;
        this.dataSource.setMaximumPoolSize(1);

        this.jdbcOperations = new JdbcTemplate(this.dataSource);
    }

    @Override
    public void afterAll(ExtensionContext context) {
    }

    public MssqlConnectionConfiguration.Builder configBuilder() {
        return MssqlConnectionConfiguration.builder().host(getHost()).username(getUsername()).password(getPassword());
    }

    public MssqlConnectionConfiguration getConnectionConfiguration() {
        return configBuilder().build();
    }

    public HikariDataSource getDataSource() {
        return this.dataSource;
    }

    @Nullable
    public JdbcOperations getJdbcOperations() {
        return this.jdbcOperations;
    }

    public String getHost() {
        return this.sqlServer.getHost();
    }

    public int getPort() {
        return this.sqlServer.getPort();
    }

    public String getUsername() {
        return this.sqlServer.getUsername();
    }

    public String getPassword() {
        return this.sqlServer.getPassword();
    }

    /**
     * Interface to be implemented by database providers (provided database, test container).
     */
    interface DatabaseContainer {

        String getHost();

        int getPort();

        String getUsername();

        String getPassword();
    }

    /**
     * Externally provided SQL Server instance.
     */
    static class External implements DatabaseContainer {

        public static final External INSTANCE = new External();

        @Override
        public String getHost() {
            return "localhost";
        }

        @Override
        public int getPort() {
            return 1433;
        }

        @Override
        public String getUsername() {
            return "sa";
        }

        @Override
        public String getPassword() {
            return "A_Str0ng_Required_Password";
        }

        /**
         * Returns whether this container is available.
         *
         * @return
         */
        @SuppressWarnings("try")
        boolean isAvailable() {

            try (Socket ignored = new Socket(getHost(), getPort())) {

                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }

    /**
     * {@link DatabaseContainer} provided by {@link JdbcDatabaseContainer}.
     */
    static class TestContainer implements DatabaseContainer {

        private final JdbcDatabaseContainer<?> container;

        TestContainer(JdbcDatabaseContainer<?> container) {
            this.container = container;
        }

        @Override
        public String getHost() {
            return this.container.getContainerIpAddress();
        }

        @Override
        public int getPort() {
            return this.container.getMappedPort(MSSQLServerContainer.MS_SQL_SERVER_PORT);
        }

        @Override
        public String getUsername() {
            return this.container.getUsername();
        }

        @Override
        public String getPassword() {
            return this.container.getPassword();
        }
    }

}
