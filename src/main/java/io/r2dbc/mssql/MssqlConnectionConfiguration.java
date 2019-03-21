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

import io.r2dbc.mssql.util.Assert;
import io.r2dbc.mssql.util.StringUtils;
import reactor.util.annotation.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.UUID;

/**
 * Connection configuration information for connecting to a Microsoft SQL database.
 * Allows configuration of the connection endpoint, login credentials, database and trace details such as application name and connection Id.
 *
 * @author Mark Paluch
 */
public final class MssqlConnectionConfiguration {

    @Nullable
    private final UUID connectionId;

    private final String database;

    private final String host;

    private final String password;

    private final int port;

    private final String username;

    @Nullable
    private final String appName;

    private final boolean ssl;

    private MssqlConnectionConfiguration(@Nullable UUID connectionId, @Nullable String database, String host,
                                         String password, int port, String username, String appName, boolean ssl) {

        this.connectionId = connectionId;
        this.database = database;
        this.host = Assert.requireNonNull(host, "host must not be null");
        this.password = Assert.requireNonNull(password, "password must not be null");
        this.port = port;
        this.username = Assert.requireNonNull(username, "username must not be null");
        this.appName = appName;
        this.ssl = ssl;
    }

    /**
     * Returns a new {@link Builder}.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [connectionId=").append(this.connectionId);
        sb.append(", database=\"").append(this.database).append('\"');
        sb.append(", host=\"").append(this.host).append('\"');
        sb.append(", password=\"").append(this.password.replaceAll("|", "\\*")).append('\"');
        sb.append(", port=").append(this.port);
        sb.append(", username=\"").append(this.username).append('\"');
        sb.append(", appName=\"").append(this.appName).append('\"');
        sb.append(", encryption=").append(this.ssl);
        sb.append(']');
        return sb.toString();
    }

    @Nullable
    UUID getConnectionId() {
        return this.connectionId;
    }

    Optional<String> getDatabase() {
        return Optional.ofNullable(this.database);
    }

    String getHost() {
        return this.host;
    }

    String getPassword() {
        return this.password;
    }

    int getPort() {
        return this.port;
    }

    String getUsername() {
        return this.username;
    }

    @Nullable
    String getAppName() {
        return this.appName;
    }

    boolean useSsl() {
        return ssl;
    }

    LoginConfiguration getLoginConfiguration() {
        return new LoginConfiguration(getUsername(), getPassword(), getDatabase().orElse(""), lookupHostName(),
            getAppName(), getHost(), this.connectionId, useSsl());
    }

    /**
     * Looks up local hostname of client machine.
     *
     * @return hostname string or IP of host if hostname cannot be resolved. If neither hostname or IP found returns an empty string.
     */
    private static String lookupHostName() {

        try {
            InetAddress localAddress = InetAddress.getLocalHost();
            if (localAddress != null) {
                String value = localAddress.getHostName();
                if (StringUtils.hasText(value)) {
                    return value;
                }

                value = localAddress.getHostAddress();
                if (StringUtils.hasText(value)) {
                    return value;
                }
            }
        } catch (UnknownHostException e) {
            return "";
        }
        // If hostname not found, return standard "" string.
        return "";
    }

    /**
     * A builder for {@link MssqlConnectionConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        private UUID connectionId = UUID.randomUUID();

        private String database;

        private String host;

        private String password;

        private int port = 1433;

        private String username;

        private String appName;

        private boolean ssl;

        private Builder() {
        }

        /**
         * Configure the connectionId.
         *
         * @param connectionId the application name
         * @return this {@link Builder}
         * @throws IllegalArgumentException when {@link UUID} is {@code null}.
         */
        public Builder connectionId(UUID connectionId) {
            this.connectionId = Assert.requireNonNull(connectionId, "connectionId must not be null");
            return this;
        }

        /**
         * Configure the appName.
         *
         * @param appName the appName
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code appName} is {@code null}
         */
        public Builder appName(String appName) {
            this.appName = Assert.requireNonNull(appName, "appName must not be null");
            return this;
        }

        /**
         * Configure the database.
         *
         * @param database the database
         * @return this {@link Builder}
         */
        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

        /**
         * Enable SSL usage. This flag is also known as Use Encryption in other drivers.
         *
         * @return this {@link Builder}
         */
        public Builder enableSsl() {
            this.ssl = true;
            return this;
        }

        /**
         * Configure the host.
         *
         * @param host the host
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code host} is {@code null}
         */
        public Builder host(String host) {
            this.host = Assert.requireNonNull(host, "host must not be null");
            return this;
        }

        /**
         * Configure the username.
         *
         * @param username the username
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code username} is {@code null}
         */
        public Builder username(String username) {
            this.username = Assert.requireNonNull(username, "username must not be null");
            return this;
        }

        /**
         * Configure the password.
         *
         * @param password the password
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code password} is {@code null}
         */
        public Builder password(String password) {
            this.password = Assert.requireNonNull(password, "password must not be null");
            return this;
        }

        /**
         * Configure the port. Defaults to {@code 5432}.
         *
         * @param port the port
         * @return this {@link Builder}
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Returns a configured {@link MssqlConnectionConfiguration}.
         *
         * @return a configured {@link MssqlConnectionConfiguration}
         */
        public MssqlConnectionConfiguration build() {
            return new MssqlConnectionConfiguration(this.connectionId, this.database, this.host, this.password, this.port,
                this.username, this.appName, this.ssl);
        }
    }
}
