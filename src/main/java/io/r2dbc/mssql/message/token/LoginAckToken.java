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
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.util.Version;

/**
 * Info token.
 *
 * @author Mark Paluch
 */
public class LoginAckToken extends AbstractDataToken {

    public static final byte TYPE = (byte) 0xAD;

    public static final byte CLIENT_INTEFACE_DEFAULT = 0;

    public static final byte CLIENT_INTEFACE_TSQL = 1;

    /*
     * The total length, in bytes, of the following fields: Interface, TDSVersion, Progname, and ProgVersion.
     */
    private final long length;

    /**
     * The type of interface with which the server will accept client requests:
     * <ul>
     * <li>SQL_DFLT (server confirms that whatever is sent by the client is acceptable. If the client requested SQL_DFLT,
     * SQL_TSQL will be used)</li>
     * <li>SQL_TSQL (TSQL is accepted)</li>
     * </ul>
     */
    private final byte clientInterface;

    /**
     * The TDS version being used by the server.
     */
    private final int tdsVersion;

    /**
     * The name of the server.
     */
    private final String progrName;

    /**
     * Server version.
     */
    private final Version version;

    public LoginAckToken(long length, byte clientInterface, int tdsVersion, String progrName, Version version) {
        super(TYPE);
        this.length = length;
        this.clientInterface = clientInterface;
        this.tdsVersion = tdsVersion;
        this.progrName = progrName;
        this.version = version;
    }

    /**
     * Decode the {@link LoginAckToken}.
     *
     * @param buffer
     * @param version
     * @return
     */
    public static LoginAckToken decode(ByteBuf buffer) {

        int length = Decode.uShort(buffer);
        byte clientInterface = Decode.asByte(buffer);
        int tdsVersion = Decode.intBigEndian(buffer);

        String progName = Decode.unicodeBString(buffer);
        int major = Decode.asByte(buffer);
        int minor = Decode.asByte(buffer);
        int build = buffer.readShort();

        Version serverVersion = new Version(major, minor, build);

        return new LoginAckToken(length, clientInterface, tdsVersion, progName, serverVersion);
    }

    public byte getClientInterface() {
        return this.clientInterface;
    }

    public int getTdsVersion() {
        return this.tdsVersion;
    }

    public String getProgrName() {
        return this.progrName;
    }

    public Version getVersion() {
        return this.version;
    }

    @Override
    public String getName() {
        return "LOGINACK";
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [clientInterface=").append(this.clientInterface);
        sb.append(", tdsVersion=").append(this.tdsVersion);
        sb.append(", progrName='").append(this.progrName).append('\"');
        sb.append(", version=").append(this.version);
        sb.append(']');
        return sb.toString();
    }
}
