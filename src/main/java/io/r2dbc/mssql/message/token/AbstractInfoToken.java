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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.util.Assert;

/**
 * Info token.
 *
 * @author Mark Paluch
 */
public abstract class AbstractInfoToken extends AbstractDataToken {

    /*
     * The total length of the INFO data stream, in bytes.
     */
    private final long length;

    /**
     * Info number.
     */
    private final long number;

    /**
     * The error state, used as a modifier to the info Number.
     */
    private final byte state;

    /**
     * The class (severity) of the error. A class of less than 10 indicates an informational message.
     */
    private final byte infoClass;

    /**
     * Classification constant.
     */
    private final Classification classification;

    /**
     * The message text length and message text using US_VARCHAR format.
     */
    private final String message;

    /**
     * The server name length and server name using B_VARCHAR format.
     */
    private final String serverName;

    /**
     * The stored procedure name length and stored procedure name using B_VARCHAR format.
     */
    private final String procName;

    /**
     * The line number in the SQL batch or stored procedure that caused the error.
     * <p/>
     * Line numbers begin at 1; therefore, if the line number is not applicable to the message as determined by the upper
     * layer, the value of LineNumber will be 0.
     */
    private final long lineNumber;

    AbstractInfoToken(byte type, long length, long number, byte state, byte infoClass, String message, String serverName,
                      String procName, long lineNumber) {

        super(type);
        this.length = length;
        this.number = number;
        this.state = state;
        this.infoClass = infoClass;
        this.classification = Classification.valueOf(this.infoClass);
        this.message = message;
        this.serverName = serverName;
        this.procName = procName;
        this.lineNumber = lineNumber;
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link AbstractInfoToken}.
     *
     * @param buffer the data buffer.
     * @return {@code true} if the buffer contains sufficient data to entirely decode a {@link AbstractInfoToken}.
     */
    public static boolean canDecode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        Integer requiredLength = Decode.peekUShort(buffer);

        return requiredLength != null && buffer.readableBytes() >= (requiredLength + /* length field */ 2);
    }

    public long getNumber() {
        return this.number;
    }

    public byte getState() {
        return this.state;
    }

    public byte getInfoClass() {
        return this.infoClass;
    }

    public Classification getClassification() {
        return this.classification;
    }

    public String getMessage() {
        return this.message;
    }

    public String getServerName() {
        return this.serverName;
    }

    public String getProcName() {
        return this.procName;
    }

    public long getLineNumber() {
        return this.lineNumber;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [number=").append(this.number);
        sb.append(", state=").append(this.state);
        sb.append(", infoClass=").append(this.infoClass);
        sb.append(", message='").append(this.message).append('\"');
        sb.append(", serverName='").append(this.serverName).append('\"');
        sb.append(", procName='").append(this.procName).append('\"');
        sb.append(", lineNumber=").append(this.lineNumber);
        sb.append(']');
        return sb.toString();
    }

    public enum Classification {

        /**
         * Informational messages that return status information or report errors that are not severe.
         */
        INFORMATIONAL(0, 10),

        /**
         * The given object or entity does not exist.
         */
        OBJECT_DOES_NOT_EXIST(11),

        /**
         * A special severity for SQL statements that do not use locking because of special options. In some cases, read operations performed by these SQL statements could result in inconsistent
         * data, because locks are not taken to guarantee consistency.
         */
        INCONSISTENT_NO_LOCK(12),

        /**
         * Transaction deadlock errors.
         */
        TX_DEADLOCK(13),

        /**
         * Security-related errors, such as permission denied.
         */
        SECURITY(14),

        /**
         * Syntax errors in the SQL statement.
         */
        SYNTAX_ERROR(15),

        /**
         * General errors that can be corrected by the user.
         */
        GENERAL_ERROR(16),

        /**
         * The SQL statement caused the database server to run out of resources (such as memory, locks, or disk space for the database) or to exceed some limit set by the system administrator.
         */
        OUT_OF_RESOURCES(17),

        /**
         * There is a problem in the Database Engine software, but the SQL statement completes execution, and the connection to the instance of the Database Engine is maintained. System
         * administrator action is required.
         */
        DATABASE_ENGINE_FAILURE(18),

        /**
         * A non-configurable Database Engine limit has been exceeded and the current SQL batch has been terminated. Error messages with a severity level of 19 or higher stop the execution of the
         * current SQL batch.
         */
        DATABASE_LIMIT(19),

        /**
         * Indicates that a SQL statement has encountered a problem. Because the problem has affected only the current task, it is unlikely that the database itself has been damaged.
         */
        SYSTEM_SQL_PROBLEM(20),

        /**
         * Indicates that a problem has been encountered that affects all tasks in the current database, but it is unlikely that the database itself has been damaged.
         */
        ALL_TASKS_PROBLEM(21),

        /**
         * Indicates that the table or index specified in the message has been damaged by a software or hardware problem.
         */
        INDEX_PROBLEM(22),

        /**
         * Indicates that the integrity of the entire database is in question because of a hardware or software problem.
         */
        DATABASE_INTEGRITY_PROBLEM(23),

        /**
         * Indicates a media failure. The system administrator might have to restore the database or resolve a hardware issue.
         */
        MEDIA_ERROR(24),

        /**
         * Unknown classification.
         */
        UNKNOWN(-1);

        final int from;

        final int to;

        Classification(int code) {
            this(code, code);
        }

        Classification(int from, int to) {
            this.from = from;
            this.to = to;
        }

        /**
         * Lookup classification by its class value.
         *
         * @param value the class value.
         * @return the matching {@link Classification} or {@link Classification#UNKNOWN} if it cannot be resolved.
         */
        static Classification valueOf(int value) {

            for (Classification classification : Classification.values()) {
                if (value >= classification.from && value <= classification.to) {
                    return classification;
                }
            }

            return Classification.UNKNOWN;
        }
    }

}
