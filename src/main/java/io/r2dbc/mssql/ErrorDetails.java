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

import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.InfoToken;
import io.r2dbc.mssql.util.StringUtils;
import reactor.util.annotation.Nullable;

/**
 * Details for an Error.
 *
 * @author Mark Paluch
 * @see ErrorToken
 * @see InfoToken
 */
public final class ErrorDetails {

    /**
     * Error message.
     */
    private final String message;

    /**
     * Info number.
     */
    private final long number;

    /**
     * The error state, used as a modifier to the info Number.
     */
    private final int state;

    /**
     * The class (severity) of the error. A class of less than 10 indicates an informational message.
     */
    private final int infoClass;

    /**
     * The server name length and server name using B_VARCHAR format.
     */
    @Nullable
    private final String serverName;

    /**
     * The stored procedure name length and stored procedure name using B_VARCHAR format.
     */
    @Nullable
    private final String procName;

    /**
     * The line number in the SQL batch or stored procedure that caused the error.
     * Line numbers begin at 1; therefore, if the line number is not applicable to the message as determined by the upper
     * layer, the value of LineNumber will be 0.
     */
    private final long lineNumber;

    /**
     * Create {@link ErrorDetails}.
     *
     * @param message    the exception message.
     * @param number     the error number.
     * @param state      SQL state.
     * @param infoClass  message classification.
     * @param serverName name of the server.
     * @param procName   procedure name.
     * @param lineNumber line number in the offending SQL.
     */
    public ErrorDetails(String message, long number, int state, int infoClass, String serverName, String procName,
                        long lineNumber) {
        this.message = message;
        this.number = number;
        this.state = state;
        this.infoClass = infoClass;
        this.serverName = StringUtils.hasText(serverName) ? serverName : null;
        this.procName = procName;
        this.lineNumber = lineNumber;
    }

    /**
     * Returns the error message.
     *
     * @return the error message.
     */
    public String getMessage() {
        return message;
    }

    /**
     * Returns the message number.
     *
     * @return the message number.
     */
    public long getNumber() {
        return this.number;
    }

    /**
     * The error state, used as a modifier to the message number.
     *
     * @return the error state.
     */
    public int getState() {
        return this.state;
    }

    /**
     * The error state code.
     *
     * @return the error state.
     */
    public String getStateCode() {
        return getStateCode((int) getNumber(), getState());
    }

    /**
     * Returns the severity class of this {@link ErrorDetails}.
     *
     * @return severity class of this {@link ErrorDetails}.
     */
    public int getInfoClass() {
        return this.infoClass;
    }

    /**
     * Returns the server name.
     *
     * @return the server name.
     */
    @Nullable
    public String getServerName() {
        return this.serverName;
    }

    /**
     * Returns the procedure name.
     *
     * @return the procedure name.
     */
    @Nullable
    public String getProcName() {
        return this.procName;
    }

    /**
     * The line number in the SQL batch or stored procedure that caused the error. Line numbers begin at 1; therefore, if the line number is not applicable to the message as determined by the upper
     * layer, the value of LineNumber will be 0.
     *
     * @return the line number in the SQL batch.
     */
    public long getLineNumber() {
        return this.lineNumber;
    }

    private String getStateCode(int errNum, int databaseState) {

        switch (errNum) {
            // case 18456: return "08001"; //username password wrong at login
            case 8152:
                return "22001"; // String data right truncation
            case 515: // 2.2705
            case 547:
                return "23000";  // Integrity constraint violation
            case 2601:
                return "23000";  // Integrity constraint violation
            case 2714:
                return "S0001"; // table already exists
            case 208:
                return "S0002";  // table not found
            case 1205:
                return "40001"; // deadlock detected
            case 2627:
                return "23000"; // DPM 4.04. Primary key violation
        }
        return "S000" + databaseState;
    }
}
