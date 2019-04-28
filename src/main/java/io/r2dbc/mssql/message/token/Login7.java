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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.TDSVersion;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.TdsFragment;
import io.r2dbc.mssql.message.tds.TdsPackets;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.mssql.util.DriverVersion;
import io.r2dbc.mssql.util.Version;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;

/**
 * Login 7 message.
 *
 * @author Mark Paluch
 */
public final class Login7 implements TokenStream, ClientMessage {

    private static final short TDS_LOGIN_REQUEST_BASE_LEN = 94;

    private static final HeaderOptions header = HeaderOptions.create(Type.TDS7_LOGIN, Status.empty());

    private final int estimatedPacketLength;

    private final int baseLength;

    /**
     * The highest TDS version being used by the client (for example, 0x00000071 for TDS 7.1).
     * <p/>
     * If the TDSVersion value sent by the client is greater than the value that the server recognizes, the server MUST
     * use the highest TDS version that it can use. This provides a mechanism for clients to discover the server TDS by
     * sending a standard LOGIN7 message. If the TDSVersion value sent by the client is lower than the highest TDS version
     * the server recognizes, the server MUST use the TDS version sent by the client
     */
    private final TDSVersion tdsVersion;

    /**
     * The packet size being requested by the client.
     */
    private final int packetSize;

    /**
     * The version of the interface library (for example, ODBC or OLEDB) being used by the client, 4 byte.
     */
    private final byte[] clientProgVer;

    /**
     * The process ID of the client application.
     */
    private final int clientPid;

    /**
     * The connection ID of the primary Server. Used when connecting to an "Always Up" backup server.
     */
    private final int connectionId;

    private final OptionFlags1 optionFlags1;

    private final OptionFlags2 optionFlags2;

    private final TypeFlags typeFlags;

    private final OptionFlags3 optionFlags3;

    private final Collection<LoginRequestToken> tokens;

    private final byte[] clientId;

    private final ConditionalProtocolSegment passwordChange;

    private Login7(TDSVersion tdsVersion, int packetSize, byte[] clientProgVer, int clientPid, int connectionId,
                   OptionFlags1 optionFlags1, OptionFlags2 optionFlags2, TypeFlags typeFlags, OptionFlags3 optionFlags3,
                   Collection<LoginRequestToken> tokens, byte[] clientId) {

        this.tdsVersion = tdsVersion;
        this.packetSize = packetSize;
        this.clientProgVer = clientProgVer;
        this.clientPid = clientPid;
        this.connectionId = connectionId;
        this.optionFlags1 = optionFlags1;
        this.optionFlags2 = optionFlags2;
        this.typeFlags = typeFlags;
        this.optionFlags3 = optionFlags3;
        this.tokens = tokens;
        this.clientId = clientId;

        int baseLength = TDS_LOGIN_REQUEST_BASE_LEN;

        EnumSet<TokenType> lengthRelevant = EnumSet.of(TokenType.Hostname, TokenType.AppName, TokenType.Servername,
            TokenType.IntName, TokenType.Database, TokenType.Username);
        for (LoginRequestToken token : tokens) {

            if (lengthRelevant.contains(token.getTokenType())) {
                baseLength += token.getValue().length;
            }
        }

        baseLength += getToken(TokenType.Password).getEncrypted().length;

        ConditionalProtocolSegment passwordChange = Conditionals.DISABLED;

        if (tdsVersion.isGreateOrEqualsTo(TDSVersion.VER_YUKON)) {
            passwordChange = Conditionals.PASSWORD_CHANGE;
        }

        this.passwordChange = passwordChange;
        this.baseLength = baseLength + 4 /* AE */;
        this.estimatedPacketLength = this.baseLength + Header.LENGTH + 2 + passwordChange.length() + 1;
    }

    /**
     * @return a builder for {@link Login7}.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String getName() {
        return "LOGIN7";
    }

    @Override
    public Publisher<TdsFragment> encode(ByteBufAllocator allocator, int packetSize) {

        return Mono.fromSupplier(() -> {

            ByteBuf buffer = allocator.buffer(this.estimatedPacketLength);

            encode(buffer);
            return TdsPackets.create(header, buffer);
        });
    }

    void encode(ByteBuf buffer) {

        int len = this.baseLength;
        int aeoffset = len;

        buffer.writeIntLE(this.baseLength + 6 + 1);
        buffer.writeIntLE(this.tdsVersion.getVersion());
        buffer.writeIntLE(this.packetSize);
        buffer.writeBytes(this.clientProgVer);
        buffer.writeIntLE(this.clientPid);
        buffer.writeInt(0); // Primary server connection ID
        buffer.writeByte(this.optionFlags1.getValue());
        buffer.writeByte(this.optionFlags2.getValue());
        buffer.writeByte(this.typeFlags.getValue());
        buffer.writeByte(this.optionFlags3.getValue());
        buffer.writeIntLE(0); // Client time zone
        buffer.writeIntLE(0); // Client LCID

        int dataLen = 0;

        LoginRequestToken hostname = getToken(TokenType.Hostname);
        LoginRequestToken username = getToken(TokenType.Username);
        LoginRequestToken password = getToken(TokenType.Password);
        LoginRequestToken appName = getToken(TokenType.AppName);
        LoginRequestToken serverName = getToken(TokenType.Servername);
        LoginRequestToken intName = getToken(TokenType.IntName);
        LoginRequestToken database = getToken(TokenType.Database);

        // Hostname position + length
        dataLen = writeToken(dataLen, buffer, hostname, LoginRequestToken::getValue);

        // Credentials position + length
        dataLen = writeToken(dataLen, buffer, username, LoginRequestToken::getValue);
        dataLen = writeToken(dataLen, buffer, password, LoginRequestToken::getEncrypted);

        // AppName position + length
        dataLen = writeToken(dataLen, buffer, appName, LoginRequestToken::getValue);

        // Server name position + length
        dataLen = writeToken(dataLen, buffer, serverName, LoginRequestToken::getValue);

        // Unused
        // AE is always ON
        buffer.writeShortLE(TDS_LOGIN_REQUEST_BASE_LEN + dataLen);
        buffer.writeShortLE(4);
        dataLen += 4;

        // Interface library name position + length
        dataLen = writeToken(dataLen, buffer, intName, LoginRequestToken::getValue);

        // Language
        buffer.writeShort(0);
        buffer.writeShort(0);

        // Database name position + length
        dataLen = writeToken(dataLen, buffer, database, LoginRequestToken::getValue);

        buffer.writeBytes(this.clientId);

        // SSPI/Integrated security disabled.
        buffer.writeShort(0);
        buffer.writeShort(0);

        // Database to attach during connection process
        buffer.writeShort(0);
        buffer.writeShort(0);

        // TDS 7.2: Password change
        this.passwordChange.encode(buffer);

        buffer.writeBytes(hostname.getValue());
        buffer.writeBytes(username.getValue());
        buffer.writeBytes(password.getEncrypted());
        buffer.writeBytes(appName.getValue());
        buffer.writeBytes(serverName.getValue());

        // Extension disabled
        buffer.writeIntLE(aeoffset);

        buffer.writeBytes(intName.getValue());
        buffer.writeBytes(database.getValue());

        // AE
        buffer.writeByte(4);
        buffer.writeIntLE(1);
        buffer.writeByte(1);

        // AE Terminator
        buffer.writeByte(-1);
    }

    private int writeToken(int dataLength, ByteBuf buffer, LoginRequestToken token,
                           Function<LoginRequestToken, byte[]> valueFunction) {

        buffer.writeShortLE(TDS_LOGIN_REQUEST_BASE_LEN + dataLength);
        buffer.writeShortLE(token.getLength());

        return dataLength + valueFunction.apply(token).length;
    }

    private LoginRequestToken getToken(TokenType tokenType) {

        for (LoginRequestToken token : this.tokens) {
            if (token.getTokenType() == tokenType) {
                return token;
            }
        }

        return new LoginRequestToken(TokenType.Unknown, "");
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [header=").append(header);
        sb.append(", tdsVersion=").append(this.tdsVersion);
        sb.append(", packetSize=").append(this.packetSize);
        sb.append(", clientPid=").append(this.clientPid);
        sb.append(", connectionId=").append(this.connectionId);
        sb.append(", tokens=").append(this.tokens);
        sb.append(']');
        return sb.toString();
    }

    /**
     * Builder for {@link Login7} requests. Pre-initializes driver version, driver name, app name, and hostname.
     */
    public static class Builder {

        private TDSVersion tdsVersion;

        /**
         * The packet size being requested by the client.
         */
        private int packetSize = 8000;

        /**
         * The version of the interface library (for example, ODBC or OLEDB) being used by the client.
         */
        private Version clientLibraryVersion = DriverVersion.getVersion();

        /**
         * The process ID of the client application.
         */
        private int clientPid;

        /**
         * The client Id.
         */
        private byte[] clientId = new byte[6];

        /**
         * The connection ID of the primary Server. Used when connecting to an "Always Up" backup server.
         */
        private int connectionId;

        private OptionFlags1 optionFlags1 = OptionFlags1.empty().byteOrderX86().charSetAscii().floatIeee754().dumpLoadOn()
            .useDbOff().initDatabaseFailFatal().enableLang();

        private OptionFlags2 optionFlags2 = OptionFlags2.empty().setInitLangFailFatal().enableOdbc()
            .disableIntegratedSecurity();

        private TypeFlags typeFlags = TypeFlags.empty().defaultSqlType();

        private OptionFlags3 optionFlags3 = OptionFlags3.empty().enableUnknownCollationHandling().enableExtensions();

        @Nullable
        private CharSequence username;

        @Nullable
        private CharSequence password;

        @Nullable
        private CharSequence applicationName;

        @Nullable
        private CharSequence hostname;

        private CharSequence clientLibraryName;

        @Nullable
        private CharSequence databaseName;

        @Nullable
        private CharSequence serverName;

        private Builder() {

            String clientLibraryName = "R2DBC Driver for Microsoft SQL Server v";

            if (this.clientLibraryVersion != null) {
                clientLibraryName += this.clientLibraryVersion.toString();
            }

            this.clientLibraryName = clientLibraryName;
            this.applicationName = clientLibraryName;
        }

        /**
         * Set the TDS version.
         *
         * @param tdsVersion the TDS protocol version.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link TDSVersion} is {@code null}.
         */
        public Builder tdsVersion(TDSVersion tdsVersion) {

            this.tdsVersion = Assert.requireNonNull(tdsVersion, "TDS version must not be null");
            return this;
        }

        /**
         * Set the requested packet size.
         *
         * @param packetSize the requested packet size.
         * @return {@code this} {@link Builder}.
         */
        public Builder packetSize(int packetSize) {
            this.packetSize = packetSize;
            return this;
        }

        /**
         * Configure {@link OptionFlags1}.
         *
         * @param optionFlags1 option 1 flags.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link OptionFlags1} is {@code null}.
         */
        public Builder optionFlags1(OptionFlags1 optionFlags1) {

            this.optionFlags1 = Assert.requireNonNull(optionFlags1, "Option flags 1 must not be null");
            return this;
        }

        /**
         * Configure {@link OptionFlags2}.
         *
         * @param optionFlags2 option 2 flags.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link OptionFlags2} is {@code null}.
         */
        public Builder optionFlags2(OptionFlags2 optionFlags2) {

            this.optionFlags2 = Assert.requireNonNull(optionFlags2, "Option flags 2 must not be null");
            return this;
        }

        /**
         * Configure {@link OptionFlags3}.
         *
         * @param optionFlags3 option 3 flags.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link OptionFlags3} is {@code null}.
         */
        public Builder optionFlags3(OptionFlags3 optionFlags3) {

            this.optionFlags3 = Assert.requireNonNull(optionFlags3, "Option flags 3 must not be null");
            return this;
        }

        /**
         * Configure {@link TypeFlags}.
         *
         * @param typeFlags the type flags.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link TypeFlags} is {@code null}.
         */
        public Builder typeFlags(TypeFlags typeFlags) {

            this.typeFlags = Assert.requireNonNull(typeFlags, "Type flags must not be null");
            return this;
        }

        /**
         * Configure the user name. Must not exceed 127 chars.
         *
         * @param username login username.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@code username} is {@code null}.
         */
        public Builder username(CharSequence username) {

            this.username = Assert.requireNonNull(username, "Username must not be null");
            return this;
        }

        /**
         * Configure the password. Must not exceed 128 chars.
         *
         * @param password login password.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@code password} is {@code null}.
         */
        public Builder password(CharSequence password) {

            Assert.requireNonNull(password, "Password must not be null");
            Assert.isTrue(password.length() < 128, "Password name must be shorter than 128 chars");

            this.password = password;
            return this;
        }

        /**
         * Configure the application name. Must not exceed 128 chars.
         *
         * @param applicationName the application name.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@code applicationName} is {@code null}.
         */
        public Builder applicationName(CharSequence applicationName) {

            Assert.requireNonNull(applicationName, "App name must not be null");
            Assert.isTrue(applicationName.length() < 128, "Application name must be shorter than 128 chars");

            this.applicationName = applicationName;
            return this;
        }

        /**
         * Configure the client library name. Must not exceed 128 chars.
         *
         * @param clientLibraryName driver name.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@code clientLibraryName} is {@code null}.
         */
        public Builder clientLibraryName(CharSequence clientLibraryName) {

            Assert.requireNonNull(clientLibraryName, "Client library name must not be null");
            Assert.isTrue(clientLibraryName.length() < 128, "Client library name must be shorter than 128 chars");

            this.clientLibraryName = clientLibraryName;
            return this;
        }

        /**
         * Configure the client library version. Must not exceed 128 chars.
         *
         * @param clientLibraryVersion driver version.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link Version} is {@code null}.
         */
        public Builder clientLibraryVersion(Version clientLibraryVersion) {

            this.clientLibraryVersion = Assert.requireNonNull(clientLibraryVersion,
                "Client library version must not be null");
            return this;
        }

        /**
         * Configure the client host name. Must not exceed 128 chars.
         *
         * @param hostname the client hostname.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@code hostname} is {@code null}.
         */
        public Builder hostName(CharSequence hostname) {

            Assert.requireNonNull(hostname, "Hostname must not be null");
            Assert.isTrue(hostname.length() < 128, "Hostname name must be shorter than 128 chars");

            this.hostname = hostname;
            return this;
        }

        /**
         * Configure the database name. Must not exceed 128 chars.
         *
         * @param databaseName the initial database name.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@code databaseName} is {@code null}.
         */
        public Builder database(CharSequence databaseName) {

            Assert.requireNonNull(databaseName, "Database name must not be null");
            Assert.isTrue(databaseName.length() < 128, "Database name must be shorter than 128 chars");

            this.databaseName = databaseName;
            return this;
        }

        /**
         * Configure the client server name. Must not exceed 128 chars.
         *
         * @param serverName the remote server name.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@code serverName} is {@code null}.
         */
        public Builder serverName(CharSequence serverName) {

            Assert.requireNonNull(serverName, "Server name must not be null");
            Assert.isTrue(serverName.length() < 128, "Server name must be shorter than 128 chars");

            this.serverName = serverName;
            return this;
        }

        /**
         * Configure the client Id. Must be exactly 6 bytes.
         *
         * @param clientId the client Id.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@code clientId} is {@code null}.
         */
        public Builder clientId(byte[] clientId) {

            Assert.requireNonNull(clientId, "Client name must not be null");
            Assert.isTrue(clientId.length == 6, "Client Id must be exactly 6 chars");

            this.clientId = Arrays.copyOf(clientId, 6);
            return this;
        }

        /**
         * Configure the client process Id.
         *
         * @param processId the client process Id.
         * @return {@code this} {@link Builder}.
         */
        public Builder clientProcessId(int processId) {

            this.clientPid = processId;
            return this;
        }

        /**
         * Build a new {@link Login7} message.
         *
         * @return a new {@link Login7} message.
         * @throws IllegalStateException if {@code username}, {@code password}, or {@code databaseName} is {@code null} (unconfigured).
         */
        public Login7 build() {

            Assert.state(this.username != null, "Username must not be null");
            Assert.state(this.password != null, "Password must not be null");
            Assert.state(this.databaseName != null, "Database must not be null");

            List<LoginRequestToken> requestTokens = new ArrayList<>();
            requestTokens.add(new LoginRequestToken(TokenType.Hostname, this.hostname));
            requestTokens.add(new LoginRequestToken(TokenType.Username, this.username));
            requestTokens.add(new LoginRequestToken(TokenType.Password, this.password));
            requestTokens.add(new LoginRequestToken(TokenType.AppName, this.applicationName));
            requestTokens.add(new LoginRequestToken(TokenType.Servername, this.serverName));
            requestTokens.add(new LoginRequestToken(TokenType.IntName, this.clientLibraryName));
            requestTokens.add(new LoginRequestToken(TokenType.Database, this.databaseName));

            byte interfaceLibVersion[] = new byte[4];

            if (this.clientLibraryVersion != null) {
                interfaceLibVersion = new byte[]{(byte) 0, (byte) this.clientLibraryVersion.getBugfix(),
                    (byte) this.clientLibraryVersion.getMinor(), (byte) this.clientLibraryVersion.getMajor()};
            }

            return new Login7(this.tdsVersion, this.packetSize, interfaceLibVersion, this.clientPid, this.connectionId,
                this.optionFlags1, this.optionFlags2, this.typeFlags, this.optionFlags3, requestTokens, this.clientId);

        }
    }

    /**
     * First option byte.
     */
    public final static class OptionFlags1 {

        /**
         * fByteOrder: The byte order used by client for numeric and datetime data types.
         */
        public static final byte LOGIN_OPTION1_ORDER_X86 = 0x00;

        public static final byte LOGIN_OPTION1_ORDER_68000 = 0x01;

        /**
         * fChar: The character set used on the client.
         */
        public static final byte LOGIN_OPTION1_CHARSET_ASCII = 0x00;

        public static final byte LOGIN_OPTION1_CHARSET_EBCDIC = 0x02;

        /**
         * fFLoat: The type of floating point representation used by the client.
         */
        public static final byte LOGIN_OPTION1_FLOAT_IEEE_754 = 0x00;

        public static final byte LOGIN_OPTION1_FLOAT_VAX = 0x04;

        public static final byte LOGIN_OPTION1_FLOAT_ND5000 = 0x08;

        /**
         * fDumpLoad: Set is dump/load or BCP capabilities are needed by the client.
         */
        public static final byte LOGIN_OPTION1_DUMPLOAD_ON = 0x00;

        public static final byte LOGIN_OPTION1_DUMPLOAD_OFF = 0x10;

        /**
         * UseDB: Set if the client requires warning messages on execution of the USE SQL statement. If this flag is not
         * set, the server MUST NOT inform the client when the database changes, and therefore the client will be unaware of
         * any accompanying collation changes.
         */
        public static final byte LOGIN_OPTION1_USE_DB_ON = 0x00;

        public static final byte LOGIN_OPTION1_USE_DB_OFF = 0x20;

        /**
         * fDatabase: Set if the change to initial database needs to succeed if the connection is to succeed.
         */
        public static final byte LOGIN_OPTION1_INIT_DB_WARN = 0x00;

        public static final byte LOGIN_OPTION1_INIT_DB_FATAL = 0x40;

        /**
         * fSetLang: Set if the client requires warning messages on execution of a language change statement.
         */
        public static final byte LOGIN_OPTION1_SET_LANG_OFF = 0x00;

        public static final byte LOGIN_OPTION1_SET_LANG_ON = (byte) 0x80;

        private final int optionByte;

        private OptionFlags1(int optionByte) {
            this.optionByte = optionByte;
        }

        /**
         * Creates an empty {@link OptionFlags1}.
         *
         * @return a new {@link OptionFlags1}.
         */
        public static OptionFlags1 empty() {
            return new OptionFlags1((byte) 0x00);
        }

        /**
         * Enable x68 byte order.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 byteOrderX86() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_ORDER_X86);
        }

        /**
         * Enable 68000 byte order.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 byteOrder6800() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_ORDER_68000);
        }

        /**
         * Enable ASCII charset use.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 charSetAscii() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_CHARSET_ASCII);
        }

        /**
         * Enable EBCDIC charset use.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 charSetEbcdic() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_CHARSET_EBCDIC);
        }

        /**
         * Represent floating point numbers using IEE 754.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 floatIeee754() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_FLOAT_IEEE_754);
        }

        /**
         * Represent floating point numbers using VAX representation.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 floatVax() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_FLOAT_VAX);
        }

        /**
         * Represent floating point numbers using ND500.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 floatNd500() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_FLOAT_ND5000);
        }

        /**
         * Enable dump (BCP) loading capabilities.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 dumpLoadOn() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_DUMPLOAD_ON);
        }

        /**
         * Disable dump (BCP) loading capabilities.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 dumpLoadOff() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_DUMPLOAD_OFF);
        }

        /**
         * Disable {@code USE <database>} usage.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 useDbOff() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_USE_DB_OFF);
        }

        /**
         * Enable {@code USE <database>} usage.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 useDbOn() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_USE_DB_ON);
        }

        /**
         * Warn if the initial database cannot be used (selected).
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 initDatabaseFailWarn() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_INIT_DB_WARN);
        }

        /**
         * Fail if the initial database cannot be used (selected).
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 initDatabaseFailFatal() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_INIT_DB_FATAL);
        }

        /**
         * Disable language change.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 disableLang() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_SET_LANG_OFF);
        }

        /**
         * Enable language change.
         *
         * @return new {@link OptionFlags1} with the option applied.
         */
        public OptionFlags1 enableLang() {
            return new OptionFlags1(this.optionByte | LOGIN_OPTION1_SET_LANG_ON);
        }

        /**
         * @return the combined option byte.
         */
        public byte getValue() {
            return (byte) this.optionByte;
        }
    }

    /**
     * Second option byte.
     */
    public final static class OptionFlags2 {

        /**
         * fLanguage: Set if the change to initial language needs to succeed if the connect is to succeed.
         */
        public static final byte LOGIN_OPTION2_INIT_LANG_WARN = 0x00;

        public static final byte LOGIN_OPTION2_INIT_LANG_FATAL = 0x01;

        /**
         * fODBC: Set if the client is the ODBC driver. This causes the server to set ANSI_DEFAULTS to ON,
         * CURSOR_CLOSE_ON_COMMIT and IMPLICIT_TRANSACTIONS to OFF, TEXTSIZE to 0x7FFFFFFF (2GB) (TDS 7.2 and earlier),
         * TEXTSIZE to infinite (introduced in TDS 7.3), and ROWCOUNT to infinite.
         */
        public static final byte LOGIN_OPTION2_ODBC_OFF = 0x00;

        public static final byte LOGIN_OPTION2_ODBC_ON = 0x02;

        public static final byte LOGIN_OPTION2_TRAN_BOUNDARY_OFF = 0x00;

        public static final byte LOGIN_OPTION2_TRAN_BOUNDARY_ON = 0x04;

        public static final byte LOGIN_OPTION2_CACHE_CONNECTION_OFF = 0x00;

        public static final byte LOGIN_OPTION2_CACHE_CONNECTION_ON = 0x08;

        /**
         * fUserType: The type of user connecting to the server.
         */
        public static final byte LOGIN_OPTION2_USER_NORMAL = 0x00;

        public static final byte LOGIN_OPTION2_USER_SERVER = 0x10;

        public static final byte LOGIN_OPTION2_USER_REMUSER = 0x20;

        public static final byte LOGIN_OPTION2_USER_SQLREPL = 0x30;

        /**
         * fIntSecurity: The type of security required by the client.
         */
        public static final byte LOGIN_OPTION2_INTEGRATED_SECURITY_OFF = 0x00;

        public static final byte LOGIN_OPTION2_INTEGRATED_SECURITY_ON = (byte) 0x80;

        private final int optionByte;

        private OptionFlags2(int optionByte) {
            this.optionByte = optionByte;
        }

        /**
         * Creates an empty {@link OptionFlags2}.
         *
         * @return new {@link OptionFlags2}.
         */
        public static OptionFlags2 empty() {
            return new OptionFlags2((byte) 0x00);
        }

        /**
         * Warn if initial language cannot be selected.
         *
         * @return new {@link OptionFlags2} with the option applied.
         */
        public OptionFlags2 setInitLangFailWarn() {
            return new OptionFlags2(this.optionByte | LOGIN_OPTION2_INIT_LANG_WARN);
        }

        /**
         * Fail if initial language cannot be selected.
         *
         * @return new {@link OptionFlags2} with the option applied.
         */
        public OptionFlags2 setInitLangFailFatal() {
            return new OptionFlags2(this.optionByte | LOGIN_OPTION2_INIT_LANG_FATAL);
        }

        /**
         * Enable ODBC use.
         *
         * @return new {@link OptionFlags2} with the option applied.
         */
        public OptionFlags2 disableOdbc() {
            return new OptionFlags2(this.optionByte | LOGIN_OPTION2_ODBC_OFF);
        }

        /**
         * Enable ODBC use.
         *
         * @return new {@link OptionFlags2} with the option applied.
         */
        public OptionFlags2 enableOdbc() {
            return new OptionFlags2(this.optionByte | LOGIN_OPTION2_ODBC_ON);
        }

        /**
         * Use regular login.
         *
         * @return new {@link OptionFlags2} with the option applied.
         */
        public OptionFlags2 normalUserType() {
            return new OptionFlags2(this.optionByte | LOGIN_OPTION2_USER_NORMAL);
        }

        /**
         * (reserved).
         *
         * @return new {@link OptionFlags2} with the option applied.
         */
        public OptionFlags2 serverUserType() {
            return new OptionFlags2(this.optionByte | LOGIN_OPTION2_USER_SERVER);
        }

        /**
         * Use remote (distributed query login) login.
         *
         * @return new {@link OptionFlags2} with the option applied.
         */
        public OptionFlags2 remoteUserType() {
            return new OptionFlags2(this.optionByte | LOGIN_OPTION2_USER_REMUSER);
        }

        /**
         * Use replication login.
         *
         * @return new {@link OptionFlags2} with the option applied.
         */
        public OptionFlags2 sqlreplUserType() {
            return new OptionFlags2(this.optionByte | LOGIN_OPTION2_USER_SQLREPL);
        }

        /**
         * Disable integrated security.
         *
         * @return new {@link OptionFlags2} with the option applied.
         */
        public OptionFlags2 disableIntegratedSecurity() {
            return new OptionFlags2(this.optionByte | LOGIN_OPTION2_INTEGRATED_SECURITY_OFF);
        }

        /**
         * Enable integrated security.
         *
         * @return new {@link OptionFlags2} with the option applied.
         */
        public OptionFlags2 enableIntegratedSecurity() {
            return new OptionFlags2(this.optionByte | LOGIN_OPTION2_INTEGRATED_SECURITY_ON);
        }

        /**
         * @return the combined option byte.
         */
        public byte getValue() {
            return (byte) this.optionByte;
        }
    }

    /**
     * Type flags.
     */
    public final static class TypeFlags {

        /**
         * fSQLType: The type of SQL the client sends to the server.
         */
        static final byte LOGIN_SQLTYPE_DEFAULT = 0x00;

        static final byte LOGIN_SQLTYPE_TSQL = 0x01;

        /**
         * fOLEDB: Set if the client is the OLEDB driver. This causes the server to set ANSI_DEFAULTS to ON,
         * CURSOR_CLOSE_ON_COMMIT and IMPLICIT_TRANSACTIONS to OFF, TEXTSIZE to 0x7FFFFFFF (2GB) (TDS 7.2 and earlier),
         * TEXTSIZE to infinite (introduced in TDS 7.3), and ROWCOUNT to infinite.
         */
        static final byte LOGIN_OLEDB_OFF = 0x00;

        static final byte LOGIN_OLEDB_ON = 0x10;

        /**
         * fReadOnlyIntent: This bit was introduced in TDS 7.4; however, TDS 7.1, 7.2, and 7.3 clients can also use this bit
         * in LOGIN7 to specify that the application intent of the connection is read-only. The server SHOULD ignore this
         * bit if the highest TDS version supported by the server is lower than TDS 7.4.
         */
        static final byte LOGIN_READ_ONLY_INTENT = 0x20;

        static final byte LOGIN_READ_WRITE_INTENT = 0x00;

        private final int optionByte;

        private TypeFlags(int optionByte) {
            this.optionByte = optionByte;
        }

        /**
         * Creates an empty {@link TypeFlags}.
         *
         * @return new {@link TypeFlags}.
         */
        public static TypeFlags empty() {
            return new TypeFlags((byte) 0x00);
        }

        /**
         * Use the default SQL type.
         *
         * @return new {@link TypeFlags} with the option applied.
         */
        public TypeFlags defaultSqlType() {
            return new TypeFlags(this.optionByte | LOGIN_SQLTYPE_DEFAULT);
        }

        /**
         * Use T-SQL.
         *
         * @return new {@link TypeFlags} with the option applied.
         */
        public TypeFlags tsqlType() {
            return new TypeFlags(this.optionByte | LOGIN_SQLTYPE_TSQL);
        }

        /**
         * Disable OLEDB defaults.
         *
         * @return new {@link TypeFlags} with the option applied.
         */
        public TypeFlags disableOledb() {
            return new TypeFlags(this.optionByte | LOGIN_OLEDB_OFF);
        }

        /**
         * Enable OLEDB defaults.
         *
         * @return new {@link TypeFlags} with the option applied.
         */
        public TypeFlags enableOledb() {
            return new TypeFlags(this.optionByte | LOGIN_OLEDB_ON);
        }

        /**
         * @return the combined option byte.
         */
        public byte getValue() {
            return (byte) this.optionByte;
        }
    }

    /**
     * Third option byte.
     */
    public final static class OptionFlags3 {

        /**
         * fChangePassword: Specifies whether the login request SHOULD change password.
         */
        static final byte LOGIN_OPTION3_DEFAULT = 0x00;

        static final byte LOGIN_OPTION3_CHANGE_PASSWORD = 0x01;

        /**
         * fSendYukonBinaryXML: 1 if XML data type instances are returned as binary XML.
         */
        static final byte LOGIN_OPTION3_SEND_YUKON_BINARY_XML = 0x02;

        /**
         * fUserInstance: 1 if client is requesting separate process to be spawned as user instance.
         */
        static final byte LOGIN_OPTION3_USER_INSTANCE = 0x04;

        /**
         * fUnknownCollationHandling: This bit is used by the server to determine if a client is able to properly handle
         * collations introduced after TDS 7.2. TDS 7.2 and earlier clients are encouraged to use this login packet bit.
         * Servers MUST ignore this bit when it is sent by TDS 7.3 or 7.4 clients. See [MSDN-SQLCollation] and [MS-LCID]
         * documents for the complete list of collations for a database server that supports SQL and LCIDs.
         */
        static final byte LOGIN_OPTION3_UNKNOWN_COLLATION_HANDLING = 0x08;

        /**
         * fExtension: Specifies whether ibExtension/cbExtension fields are used.
         */
        static final byte LOGIN_OPTION3_FEATURE_EXTENSION = 0x10;

        private final int optionByte;

        private OptionFlags3(int optionByte) {
            this.optionByte = optionByte;
        }

        /**
         * Creates an empty {@link OptionFlags3}.
         *
         * @return new {@link OptionFlags3}.
         */
        public static OptionFlags3 empty() {
            return new OptionFlags3((byte) 0x00);
        }

        /**
         * Request to change the password with the login.
         *
         * @return new {@link OptionFlags3} with the option applied.
         */
        public OptionFlags3 changePassword() {
            return new OptionFlags3(this.optionByte | LOGIN_OPTION3_CHANGE_PASSWORD);
        }

        /**
         * Fail if initial language cannot be selected.
         *
         * @return new {@link OptionFlags3} with the option applied.
         */
        public OptionFlags3 enableUserInstance() {
            return new OptionFlags3(this.optionByte | LOGIN_OPTION3_USER_INSTANCE);
        }

        /**
         * Enable ODBC use.
         *
         * @return new {@link OptionFlags3} with the option applied.
         */
        public OptionFlags3 enableUnknownCollationHandling() {
            return new OptionFlags3(this.optionByte | LOGIN_OPTION3_UNKNOWN_COLLATION_HANDLING);
        }

        /**
         * Enable Feature Extensions.
         *
         * @return new {@link OptionFlags3} with the option applied.
         */
        public OptionFlags3 enableExtensions() {
            return new OptionFlags3(this.optionByte | LOGIN_OPTION3_FEATURE_EXTENSION);
        }

        /**
         * @return the combined option byte.
         */
        public byte getValue() {
            return (byte) this.optionByte;
        }
    }

    public final static class LoginRequestToken {

        private final TokenType tokenType;

        private final int length;

        private final byte[] value;

        private final byte[] encrypted;

        LoginRequestToken(TokenType tokenType, @Nullable CharSequence value) {
            this.tokenType = tokenType;
            this.length = value != null ? value.length() : 0;
            this.value = toUCS16(value);
            this.encrypted = encryptPassword(value);
        }

        public TokenType getTokenType() {
            return this.tokenType;
        }

        public byte[] getValue() {
            return this.value;
        }

        public byte[] getEncrypted() {
            return this.encrypted;
        }

        public int getLength() {
            return this.length;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [tokenType=").append(this.tokenType);
            sb.append(']');
            return sb.toString();
        }

        /**
         * Convert to a String UCS16 encoding.
         *
         * @param s the string
         * @return the encoded data
         */
        private static byte[] toUCS16(@Nullable CharSequence s) {
            if (s == null) {
                return new byte[0];
            }
            int l = s.length();
            byte data[] = new byte[l * 2];
            int offset = 0;
            for (int i = 0; i < l; i++) {
                int c = s.charAt(i);
                byte b1 = (byte) (c & 0xFF);
                data[offset++] = b1;
                data[offset++] = (byte) ((c >> 8) & 0xFF); // Unicode MSB
            }
            return data;
        }

        /**
         * Encrypt a password for the SQL Server logon.
         *
         * @param pwd the password
         * @return the encryption
         */
        private byte[] encryptPassword(@Nullable CharSequence pwd) {
            // Changed to handle non ascii passwords
            if (pwd == null) {
                pwd = "";
            }
            int len = pwd.length();
            byte data[] = new byte[len * 2];
            for (int i1 = 0; i1 < len; i1++) {
                int j1 = pwd.charAt(i1) ^ 0x5a5a;
                j1 = (j1 & 0xf) << 4 | (j1 & 0xf0) >> 4 | (j1 & 0xf00) << 4 | (j1 & 0xf000) >> 4;
                byte b1 = (byte) ((j1 & 0xFF00) >> 8);
                data[(i1 * 2) + 1] = b1;
                byte b2 = (byte) ((j1 & 0x00FF));
                data[(i1 * 2) + 0] = b2;
            }
            return data;
        }
    }

    enum TokenType {
        Hostname, Username, Password, AppName, Servername, IntName, Language, Database, Unknown;
    }

    /**
     * Conditional protocol segment.
     */
    interface ConditionalProtocolSegment {

        /**
         * @return length in bytes.
         */
        int length();

        /**
         * Encode the segment onto the {@link ByteBuf}.
         *
         * @param buffer the target {@link ByteBuf}.
         */
        void encode(ByteBuf buffer);
    }

    /**
     * Collection of {@link ConditionalProtocolSegment}s.
     */
    enum Conditionals implements ConditionalProtocolSegment {

        DISABLED,

        /**
         * @since TDS 7.2
         */
        PASSWORD_CHANGE {
            @Override
            public int length() {
                return 8;
            }

            @Override
            public void encode(ByteBuf buffer) {
                buffer.writeShort((short) 0);
                buffer.writeShort((short) 0);
                buffer.writeInt((short) 0);
            }
        };

        @Override
        public int length() {
            return 0;
        }

        @Override
        public void encode(ByteBuf buffer) {
        }
    }
}
