/*
 * Copyright 2018-2022 the original author or authors.
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

import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.r2dbc.mssql.client.ClientConfiguration;
import io.r2dbc.mssql.client.ssl.ExpectedHostnameX509TrustManager;
import io.r2dbc.mssql.client.ssl.SslConfiguration;
import io.r2dbc.mssql.client.ssl.TrustAllTrustManager;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.message.tds.Redirect;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.mssql.util.StringUtils;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.annotation.Nullable;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Connection configuration information for connecting to a Microsoft SQL database.
 * Allows configuration of the connection endpoint, login credentials, database and trace details such as application name and connection Id.
 *
 * @author Mark Paluch
 * @author Alex Stockinger
 */
public final class MssqlConnectionConfiguration {

    /**
     * Default SQL Server port.
     */
    public static final int DEFAULT_PORT = 1433;

    /**
     * Default connect timeout.
     */
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(30);

    @Nullable
    private final String applicationName;

    @Nullable
    private final UUID connectionId;

    private final Duration connectTimeout;

    private final String database;

    private final String host;

    private final String hostNameInCertificate;

    private final CharSequence password;

    private final Predicate<String> preferCursoredExecution;

    @Nullable
    private final Duration lockWaitTimeout;

    private final int port;

    private final boolean sendStringParametersAsUnicode;

    private final boolean ssl;

    private final Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer;

    @Nullable
    private final Function<SslContextBuilder, SslContextBuilder> sslTunnelSslContextBuilderCustomizer;

    private final boolean tcpKeepAlive;

    private final boolean tcpNoDelay;

    private final boolean trustServerCertificate;

    @Nullable
    private final File trustStore;

    @Nullable
    private final String trustStoreType;

    @Nullable
    private final char[] trustStorePassword;

    private final String username;

    private MssqlConnectionConfiguration(@Nullable String applicationName, @Nullable UUID connectionId, Duration connectTimeout, @Nullable String database, String host, String hostNameInCertificate,
                                         @Nullable Duration lockWaitTimeout, CharSequence password, Predicate<String> preferCursoredExecution, int port, boolean sendStringParametersAsUnicode,
                                         boolean ssl,
                                         Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer,
                                         @Nullable Function<SslContextBuilder, SslContextBuilder> sslTunnelSslContextBuilderCustomizer, boolean tcpKeepAlive, boolean tcpNoDelay,
                                         boolean trustServerCertificate, @Nullable File trustStore, @Nullable String trustStoreType,
                                         @Nullable char[] trustStorePassword, String username) {

        this.applicationName = applicationName;
        this.connectionId = connectionId;
        this.connectTimeout = Assert.requireNonNull(connectTimeout, "connect timeout must not be null");
        this.database = database;
        this.host = Assert.requireNonNull(host, "host must not be null");
        this.hostNameInCertificate = Assert.requireNonNull(hostNameInCertificate, "hostNameInCertificate must not be null");
        this.lockWaitTimeout = lockWaitTimeout;
        this.password = Assert.requireNonNull(password, "password must not be null");
        this.preferCursoredExecution = Assert.requireNonNull(preferCursoredExecution, "preferCursoredExecution must not be null");
        this.port = port;
        this.sendStringParametersAsUnicode = sendStringParametersAsUnicode;
        this.ssl = ssl;
        this.sslContextBuilderCustomizer = sslContextBuilderCustomizer;
        this.sslTunnelSslContextBuilderCustomizer = sslTunnelSslContextBuilderCustomizer;
        this.tcpKeepAlive = tcpKeepAlive;
        this.tcpNoDelay = tcpNoDelay;
        this.trustServerCertificate = trustServerCertificate;
        this.trustStore = trustStore;
        this.trustStoreType = trustStoreType;
        this.trustStorePassword = trustStorePassword;
        this.username = Assert.requireNonNull(username, "username must not be null");
    }

    /**
     * Returns a new {@link Builder}.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a new configuration instance targeting the redirect.
     *
     * @param redirect the redirect
     * @return a new configuration instance
     * @since 0.8.2
     */
    MssqlConnectionConfiguration withRedirect(Redirect redirect) {

        String redirectServerName = redirect.getServerName();
        String hostNameInCertificate = this.hostNameInCertificate;

        // Same behavior as mssql-jdbc
        if (this.hostNameInCertificate.startsWith("*") && redirectServerName.indexOf('.') != -1) {

            // Check if redirectServerName and hostNameInCertificate are from same domain.
            boolean trustedDomain = redirectServerName.endsWith(hostNameInCertificate.substring(1));

            if (trustedDomain) {
                hostNameInCertificate = String.format("*%s", redirectServerName.substring(redirectServerName.indexOf('.')));
            }
        }

        return new MssqlConnectionConfiguration(this.applicationName, this.connectionId, this.connectTimeout, this.database, redirectServerName, hostNameInCertificate, this.lockWaitTimeout,
            this.password,
            this.preferCursoredExecution, redirect.getPort(), this.sendStringParametersAsUnicode, this.ssl, this.sslContextBuilderCustomizer,
            this.sslTunnelSslContextBuilderCustomizer, this.tcpKeepAlive, this.tcpNoDelay, this.trustServerCertificate, this.trustStore, this.trustStoreType, this.trustStorePassword, this.username);
    }

    ClientConfiguration toClientConfiguration() {
        return new DefaultClientConfiguration(this.connectTimeout, this.host, this.hostNameInCertificate, this.port, this.ssl, this.sslContextBuilderCustomizer,
            this.sslTunnelSslContextBuilderCustomizer, this.tcpKeepAlive, this.tcpNoDelay, this.trustServerCertificate, this.trustStore, this.trustStoreType, this.trustStorePassword);
    }

    ConnectionOptions toConnectionOptions() {
        return new ConnectionOptions(this.preferCursoredExecution, new DefaultCodecs(), new IndefinitePreparedStatementCache(), this.sendStringParametersAsUnicode);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [applicationName=\"").append(this.applicationName).append('\"');
        sb.append(", connectionId=").append(this.connectionId);
        sb.append(", connectTimeout=\"").append(this.connectTimeout).append('\"');
        sb.append(", database=\"").append(this.database).append('\"');
        sb.append(", host=\"").append(this.host).append('\"');
        sb.append(", hostNameInCertificate=\"").append(this.hostNameInCertificate).append('\"');
        sb.append(", lockWaitTimeout=\"").append(this.lockWaitTimeout).append('\"');
        sb.append(", password=\"").append(repeat(this.password.length(), "*")).append('\"');
        sb.append(", preferCursoredExecution=\"").append(this.preferCursoredExecution).append('\"');
        sb.append(", port=").append(this.port);
        sb.append(", sendStringParametersAsUnicode=").append(this.sendStringParametersAsUnicode);
        sb.append(", ssl=").append(this.ssl);
        sb.append(", sslContextBuilderCustomizer=").append(this.sslContextBuilderCustomizer);
        sb.append(", sslTunnelSslContextBuilderCustomizer=").append(this.sslTunnelSslContextBuilderCustomizer);
        sb.append(", tcpKeepAlive=\"").append(this.tcpKeepAlive).append("\"");
        sb.append(", tcpNoDelay=\"").append(this.tcpNoDelay).append("\"");
        sb.append(", trustServerCertificate=").append(this.trustServerCertificate);
        sb.append(", trustStore=\"").append(this.trustStore).append("\"");
        sb.append(", trustStorePassword=\"").append(repeat(this.trustStorePassword == null ? 0 : this.trustStorePassword.length, "*")).append('\"');
        sb.append(", trustStoreType=\"").append(this.trustStoreType).append("\"");
        sb.append(", username=\"").append(this.username).append('\"');
        sb.append(']');
        return sb.toString();
    }

    @Nullable
    String getApplicationName() {
        return this.applicationName;
    }

    @Nullable
    UUID getConnectionId() {
        return this.connectionId;
    }

    Duration getConnectTimeout() {
        return this.connectTimeout;
    }

    Optional<String> getDatabase() {
        return Optional.ofNullable(this.database);
    }

    String getHost() {
        return this.host;
    }

    String getHostNameInCertificate() {
        return this.hostNameInCertificate;
    }

    @Nullable
    Duration getLockWaitTimeout() {
        return this.lockWaitTimeout;
    }

    CharSequence getPassword() {
        return this.password;
    }

    Predicate<String> getPreferCursoredExecution() {
        return this.preferCursoredExecution;
    }

    int getPort() {
        return this.port;
    }

    boolean isSendStringParametersAsUnicode() {
        return this.sendStringParametersAsUnicode;
    }

    boolean useSsl() {
        return this.ssl;
    }

    boolean isTcpKeepAlive() {
        return this.tcpKeepAlive;
    }

    boolean isTcpNoDelay() {
        return this.tcpNoDelay;
    }

    String getUsername() {
        return this.username;
    }

    LoginConfiguration getLoginConfiguration() {
        return new LoginConfiguration(getApplicationName(), this.connectionId, getDatabase().orElse(""), lookupHostName(), getPassword(), getHost(), useSsl(), getUsername()
        );
    }

    private static String repeat(int length, String character) {

        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < length; i++) {
            builder.append(character);
        }

        return builder.toString();
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

        @Nullable
        private String applicationName;

        private UUID connectionId = UUID.randomUUID();

        private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;

        private String database;

        private String host;

        private String hostNameInCertificate;

        @Nullable
        private Duration lockWaitTimeout;

        private Predicate<String> preferCursoredExecution = sql -> false;

        private CharSequence password;

        private int port = DEFAULT_PORT;

        private boolean sendStringParametersAsUnicode = true;

        private boolean ssl;

        private boolean trustServerCertificate;

        private Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer = Function.identity();

        @Nullable
        private Function<SslContextBuilder, SslContextBuilder> sslTunnelSslContextBuilderCustomizer;

        private String username;

        private boolean tcpKeepAlive = false;

        private boolean tcpNoDelay = true;

        @Nullable
        private File trustStore;

        @Nullable
        private String trustStoreType;

        @Nullable
        private char[] trustStorePassword;

        private Builder() {
        }

        /**
         * Configure the applicationName.
         *
         * @param applicationName the applicationName
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code applicationName} is {@code null}
         */
        public Builder applicationName(String applicationName) {
            this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
            return this;
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
         * Configure the connect timeout. Defaults to 30 seconds.
         *
         * @param connectTimeout the connect timeout
         * @return this {@link Builder}
         */
        public Builder connectTimeout(Duration connectTimeout) {

            Assert.requireNonNull(connectTimeout, "connect timeout must not be null");
            Assert.isTrue(!connectTimeout.isNegative(), "connect timeout must not be negative");

            this.connectTimeout = connectTimeout;
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
         * Enable SSL tunnel usage to encrypt all traffic right from the connect phase. This option is required when using a SSL tunnel (e.g. stunnel or other SSL terminator) in front of the SQL
         * server and it is not related to SQL Server's built-in SSL support.
         *
         * @return this {@link Builder}
         * @since 0.8.5
         */
        public Builder enableSslTunnel() {
            return enableSslTunnel(Function.identity());
        }

        /**
         * Enable SSL tunnel usage to encrypt all traffic right from the connect phase. This option is required when using a SSL tunnel (e.g. stunnel or other SSL terminator) in front of the SQL
         * server and it is not related to SQL Server's built-in SSL support.
         * The given customizer gets applied on each SSL connection attempt to allow for just-in-time configuration updates. The {@link Function} gets
         * * called with the prepared {@link SslContextBuilder} that has all configuration options applied. The customizer may return the same builder or return a new builder instance to be used to
         * * build the SSL context.
         *
         * @param sslTunnelSslContextBuilderCustomizer customizer function
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code sslTunnelSslContextBuilderCustomizer} is {@code null}
         * @since 0.8.5
         */
        public Builder enableSslTunnel(Function<SslContextBuilder, SslContextBuilder> sslTunnelSslContextBuilderCustomizer) {
            this.sslTunnelSslContextBuilderCustomizer = Assert.requireNonNull(sslTunnelSslContextBuilderCustomizer, "sslTunnelSslContextBuilderCustomizer must not be null");
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
         * Configure the expected hostname in the SSL certificate. Defaults to {@link #host(String)} if left unconfigured. Accepts wildcards such as {@code *.database.windows.net}.
         *
         * @param hostNameInCertificate the hostNameInCertificate
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code hostNameInCertificate} is {@code null}
         */
        public Builder hostNameInCertificate(String hostNameInCertificate) {
            this.hostNameInCertificate = Assert.requireNonNull(hostNameInCertificate, "hostNameInCertificate must not be null");
            return this;
        }

        /**
         * Configure the lock wait timeout via {@code SET LOCK_TIMEOUT}. {@link Duration#isNegative() Negative values} are translated to {@code -1} meaning infinite wait.
         *
         * @param timeout the lock wait timeout
         * @return this {@link Builder}
         * @since 0.9
         */
        public Builder lockWaitTimeout(Duration timeout) {

            Assert.requireNonNull(timeout, "lock wait timeout must not be null");

            this.lockWaitTimeout = timeout;
            return this;
        }

        /**
         * Configure the password.
         *
         * @param password the password
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code password} is {@code null}
         */
        public Builder password(CharSequence password) {
            this.password = Assert.requireNonNull(password, "password must not be null");
            return this;
        }

        /**
         * Configure whether to prefer cursored execution.
         *
         * @param preferCursoredExecution {@code true} prefers cursors, {@code false} prefers direct execution. Defaults to direct execution.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code password} is {@code null}
         */
        public Builder preferCursoredExecution(boolean preferCursoredExecution) {
            return preferCursoredExecution(sql -> preferCursoredExecution);
        }

        /**
         * Configure whether to prefer cursored execution on a statement-by-statement basis. The {@link Predicate} accepts the SQL query string and returns a boolean flag indicating preference.
         * {@code true} prefers cursors, {@code false} prefers direct execution. Defaults to direct execution.
         *
         * @param preference the {@link Predicate}.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code password} is {@code null}
         */
        public Builder preferCursoredExecution(Predicate<String> preference) {
            this.preferCursoredExecution = Assert.requireNonNull(preference, "Predicate must not be null");
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
         * Configure whether to send character data as unicode (NVARCHAR, NCHAR, NTEXT) or whether to use the database encoding. Enabled by default. If disabled, {@link CharSequence} data is sent
         * using the database-specific collation such as ASCII/MBCS instead of Unicode.
         *
         * @param sendStringParametersAsUnicode {@literal true} to send character data as unicode (NVARCHAR, NCHAR, NTEXT) or whether to use the database encoding. Enabled by default.
         * @return this {@link Builder}
         */
        public Builder sendStringParametersAsUnicode(boolean sendStringParametersAsUnicode) {
            this.sendStringParametersAsUnicode = sendStringParametersAsUnicode;
            return this;
        }

        /**
         * Configure a {@link SslContextBuilder} customizer. The customizer gets applied on each SSL connection attempt to allow for just-in-time configuration updates. The {@link Function} gets
         * called with the prepared {@link SslContextBuilder} that has all configuration options applied. The customizer may return the same builder or return a new builder instance to be used to
         * build the SSL context.
         *
         * @param sslContextBuilderCustomizer customizer function
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code sslContextBuilderCustomizer} is {@code null}
         * @since 0.8.3
         */
        public Builder sslContextBuilderCustomizer(Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer) {
            this.sslContextBuilderCustomizer = Assert.requireNonNull(sslContextBuilderCustomizer, "sslContextBuilderCustomizer must not be null");
            return this;
        }

        /**
         * Configure TCP KeepAlive. Disabled by default.
         *
         * @param enabled whether to enable/disable TCP KeepAlive
         * @return this {@link Builder}
         * @see Socket#setKeepAlive(boolean)
         * @since 0.8.5
         */
        public Builder tcpKeepAlive(boolean enabled) {
            this.tcpKeepAlive = enabled;
            return this;
        }

        /**
         * Configure TCP NoDelay. Enabled by default.
         *
         * @param enabled whether to enable/disable TCP NoDelay
         * @return this {@link Builder}
         * @see Socket#setTcpNoDelay(boolean)
         * @since 0.8.5
         */
        public Builder tcpNoDelay(boolean enabled) {
            this.tcpNoDelay = enabled;
            return this;
        }

        /**
         * Allow using SSL by fully trusting the server certificate. Enabling this option skips certificate verification.
         *
         * @return this {@link Builder}.
         * @see TrustAllTrustManager
         * @since 0.8.6
         */
        public Builder trustServerCertificate() {
            return trustServerCertificate(true);
        }

        /**
         * Allow using SSL by fully trusting the server certificate. Enabling this option skips certificate verification.
         *
         * @param trustServerCertificate {@code true} to trust the server certificate without further validation.
         * @return this {@link Builder}.
         * @see TrustAllTrustManager
         * @since 0.8.6
         */
        public Builder trustServerCertificate(boolean trustServerCertificate) {
            this.trustServerCertificate = trustServerCertificate;
            return this;
        }

        /**
         * Configure the trust store type.
         *
         * @param trustStoreType the type of the trust store to be used for SSL certificate verification. Defaults to {@link KeyStore#getDefaultType()} if not set.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code trustStoreType} is {@code null}
         * @since 0.8.3
         */
        public Builder trustStoreType(String trustStoreType) {
            this.trustStoreType = Assert.requireNonNull(trustStoreType, "trustStoreType must not be null");
            return this;
        }

        /**
         * Configure the file path to the trust store.
         *
         * @param trustStoreFile the path of the trust store to be used for SSL certificate verification.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code trustStore} is {@code null}
         * @since 0.8.3
         */
        public Builder trustStore(String trustStoreFile) {
            return trustStore(new File(Assert.requireNonNull(trustStoreFile, "trustStore must not be null")));
        }

        /**
         * Configure the path to the trust store.
         *
         * @param trustStore the path of the trust store to be used for SSL certificate verification.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code trustStore} is {@code null}
         * @since 0.8.3
         */
        public Builder trustStore(File trustStore) {
            this.trustStore = Assert.requireNonNull(trustStore, "trustStore must not be null");
            return this;
        }

        /**
         * Configure the password to read the trust store.
         *
         * @param trustStorePassword the password to read the trust store.
         * @return this {@link Builder}
         * @since 0.8.3
         */
        public Builder trustStorePassword(char[] trustStorePassword) {
            this.trustStorePassword = Assert.requireNonNull(Arrays.copyOf(trustStorePassword, trustStorePassword.length), "trustStorePassword must not be null");
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
         * Returns a configured {@link MssqlConnectionConfiguration}.
         *
         * @return a configured {@link MssqlConnectionConfiguration}.
         */
        public MssqlConnectionConfiguration build() {

            if (this.hostNameInCertificate == null) {
                this.hostNameInCertificate = this.host;
            }

            return new MssqlConnectionConfiguration(this.applicationName, this.connectionId, this.connectTimeout, this.database, this.host, this.hostNameInCertificate, this.lockWaitTimeout,
                this.password,
                this.preferCursoredExecution, this.port, this.sendStringParametersAsUnicode, this.ssl, this.sslContextBuilderCustomizer,
                this.sslTunnelSslContextBuilderCustomizer, this.tcpKeepAlive,
                this.tcpNoDelay, this.trustServerCertificate, this.trustStore,
                this.trustStoreType, this.trustStorePassword, this.username);
        }

    }

    static class DefaultClientConfiguration implements ClientConfiguration {

        private final Duration connectTimeout;

        private final String host;

        private final String hostNameInCertificate;

        private final int port;

        private final boolean ssl;

        private final Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer;

        @Nullable
        private final Function<SslContextBuilder, SslContextBuilder> sslTunnelSslContextBuilderCustomizer;

        private final boolean tcpKeepAlive;

        private final boolean tcpNoDelay;

        private final boolean trustServerCertificate;

        @Nullable
        private final File trustStore;

        @Nullable
        private final String trustStoreType;

        @Nullable
        private final char[] trustStorePassword;

        DefaultClientConfiguration(Duration connectTimeout, String host, String hostNameInCertificate, int port, boolean ssl,
                                   Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer,
                                   @Nullable Function<SslContextBuilder, SslContextBuilder> sslTunnelSslContextBuilderCustomizer, boolean tcpKeepAlive, boolean tcpNoDelay,
                                   boolean trustServerCertificate, @Nullable File trustStore, @Nullable String trustStoreType, @Nullable char[] trustStorePassword) {

            this.connectTimeout = connectTimeout;
            this.host = host;
            this.hostNameInCertificate = hostNameInCertificate;
            this.port = port;
            this.ssl = ssl;
            this.sslContextBuilderCustomizer = sslContextBuilderCustomizer;
            this.sslTunnelSslContextBuilderCustomizer = sslTunnelSslContextBuilderCustomizer;
            this.tcpKeepAlive = tcpKeepAlive;
            this.tcpNoDelay = tcpNoDelay;
            this.trustServerCertificate = trustServerCertificate;
            this.trustStore = trustStore;
            this.trustStoreType = trustStoreType;
            this.trustStorePassword = trustStorePassword;
        }

        @Override
        public String getHost() {
            return this.host;
        }

        @Override
        public int getPort() {
            return this.port;
        }

        @Override
        public Duration getConnectTimeout() {
            return this.connectTimeout;
        }

        @Override
        public boolean isTcpKeepAlive() {
            return this.tcpKeepAlive;
        }

        @Override
        public boolean isTcpNoDelay() {
            return this.tcpNoDelay;
        }

        @Override
        public ConnectionProvider getConnectionProvider() {
            return ConnectionProvider.newConnection();
        }

        @Override
        public boolean isSslEnabled() {
            return this.ssl;
        }

        @Override
        public SslContext getSslContext() throws GeneralSecurityException {

            SslContextBuilder sslContextBuilder = createSslContextBuilder();

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore ks = loadCustomTrustStore();
            tmf.init(ks);

            TrustManager[] trustManagers = tmf.getTrustManagers();
            TrustManager result;

            if (isSslEnabled() && !this.trustServerCertificate) {
                result = new ExpectedHostnameX509TrustManager((X509TrustManager) trustManagers[0], this.hostNameInCertificate);
            } else {
                result = TrustAllTrustManager.INSTANCE;
            }

            sslContextBuilder.trustManager(result);

            try {
                return this.sslContextBuilderCustomizer.apply(sslContextBuilder).build();
            } catch (SSLException e) {
                throw new GeneralSecurityException(e);
            }
        }

        @Nullable
        KeyStore loadCustomTrustStore() throws GeneralSecurityException {

            if (this.trustStore == null) {
                return null;
            }

            KeyStore trustStoreInstance = KeyStore.getInstance(this.trustStoreType == null ? KeyStore.getDefaultType() : this.trustStoreType);

            try (FileInputStream fis = new FileInputStream(this.trustStore)) {
                trustStoreInstance.load(fis, this.trustStorePassword);
                return trustStoreInstance;
            } catch (IOException e) {
                throw new GeneralSecurityException(String.format("Could not load custom trust store from %s", this.trustStore), e);
            }
        }

        @Override
        public SslConfiguration getSslTunnelConfiguration() {

            if (this.sslTunnelSslContextBuilderCustomizer == null) {
                return ClientConfiguration.super.getSslTunnelConfiguration();
            }

            return new SslConfiguration() {

                @Override
                public boolean isSslEnabled() {
                    return true;
                }

                @Override
                public SslContext getSslContext() throws GeneralSecurityException {

                    SslContextBuilder sslContextBuilder = createSslContextBuilder();

                    try {
                        return DefaultClientConfiguration.this.sslTunnelSslContextBuilderCustomizer.apply(sslContextBuilder).build();
                    } catch (SSLException e) {
                        throw new GeneralSecurityException(e);
                    }
                }
            };
        }

    }

    private static SslContextBuilder createSslContextBuilder() {
        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
        sslContextBuilder.sslProvider(
                OpenSsl.isAvailable() ?
                    io.netty.handler.ssl.SslProvider.OPENSSL :
                    io.netty.handler.ssl.SslProvider.JDK)
            .ciphers(null, IdentityCipherSuiteFilter.INSTANCE)
            .applicationProtocolConfig(null);
        return sslContextBuilder;
    }

}
