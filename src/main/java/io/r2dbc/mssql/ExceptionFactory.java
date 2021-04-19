/*
 * Copyright 2019-2021 the original author or authors.
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

import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.message.token.AbstractInfoToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.InfoToken;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcRollbackException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.R2dbcTransientException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import reactor.core.publisher.SynchronousSink;

import static io.r2dbc.mssql.message.token.AbstractInfoToken.Classification.GENERAL_ERROR;

/**
 * Factory for SQL Server-specific {@link R2dbcException}s.
 *
 * @author Mark Paluch
 */
final class ExceptionFactory {

    private final String sql;

    private ExceptionFactory(String sql) {
        this.sql = sql;
    }

    /**
     * Creates a {@link ExceptionFactory} associated with a SQL query.
     *
     * @param sql
     * @return
     */
    static ExceptionFactory withSql(String sql) {
        return new ExceptionFactory(sql);
    }

    /**
     * Create a {@link R2dbcException} from {@link InfoToken}.
     *
     * @param token
     * @return
     */
    R2dbcException createException(AbstractInfoToken token) {
        return createException(token, this.sql);
    }

    /**
     * Creates a {@link R2dbcException} from an {@link AbstractInfoToken}.
     *
     * @param token the token that contains the error details.
     * @param sql   underlying SQL.
     * @return the {@link R2dbcException}.
     * @see ErrorToken
     */
    static R2dbcException createException(AbstractInfoToken token, String sql) {

        switch ((int) token.getNumber()) {
            case 106: // Too many table names in the query. The maximum allowable is %d.
            case 130: // Cannot perform an aggregate function on an expression containing an aggregate or a subquery.
            case 206: // Operand type clash: %ls is incompatible with %ls.
            case 207: // Invalid column name '%.*ls'.
            case 208: // Invalid object name '%.*ls'.
            case 209: // Ambiguous column name '%.*ls'.
            case 213: // Column name or number of supplied values does not match table definition.
            case 267: // Object '%.*ls' cannot be found.
            case 565: // A stack overflow occurred in the server while compiling the query. Please simplify the query.
            case 4408: // Too many tables. The query and the views or functions in it exceed the limit of %d tables. Revise the query to reduce the number of tables.
            case 2812: // Could not find stored procedure '%.*ls'.
                return new MssqlBadGrammarException(createErrorDetails(token), sql);

            case 2601: // Cannot insert duplicate key row in object '%.*ls' with unique index '%.*ls'. The duplicate key value is %ls.
            case 2627: // Violation of %ls constraint '%.*ls'. Cannot insert duplicate key in object '%.*ls'. The duplicate key value is %ls.
            case 544: // Cannot insert explicit value for identity column in table '%.*ls' when IDENTITY_INSERT is set to OFF.
            case 8114: // Error converting data type %ls to %ls.
            case 8115:  // Arithmetic overflow error converting %ls to data type %ls.
                return new MssqlDataIntegrityViolationException(createErrorDetails(token));

            case 701: // Maximum number of databases used for each query has been exceeded. The maximum allowed is %d.
            case 1222: // Lock request time out period exceeded.
            case 1204: // The instance of the SQL Server Database Engine cannot obtain a LOCK resource at this time. Rerun your statement when there are fewer active users. Ask the database
                // administrator to check the lock and memory configuration for this instance, or to check for
                return new MssqlTransientException(createErrorDetails(token));

            case 1203: // Process ID %d attempted to unlock a resource it does not own: %.*ls. Retry the transaction, because this error may be caused by a timing condition. If the problem
                // persists, contact the database administrator.
            case 1215:  // A conflicting ABORT_AFTER_WAIT = BLOCKERS request is waiting for existing transactions to rollback. This request cannot be executed. Please retry when the previous
                // request is completed.
            case 1216: // The DDL statement with ABORT_AFTER_WAIT = BLOCKERS option cannot be completed due to a conflicting system task. The request can abort only user transactions. Please wait
                // for the system task to complete and retry.
            case 1221: // The Database Engine is attempting to release a group of locks that are not currently held by the transaction. Retry the transaction. If the problem persists, contact your
                // support provider.
            case 1206: // The Microsoft Distributed Transaction Coordinator (MS DTC) has cancelled the distributed transaction.
            case 3938: // The transaction has been stopped because it conflicted with the execution of a FILESTREAM close operation using the same transaction.  The transaction will be rolled back.
            case 28611: // The request is aborted because the transaction has been aborted by Matrix Transaction Coordination Manager. This is mostly caused by one or more transaction particpant
                // brick went offline.
                return new MssqlRollbackException(createErrorDetails(token));

            case 921:  // Database '%.*ls' has not been recovered yet. Wait and try again.
            case 941:  // Database '%.*ls' cannot be opened because it is not started. Retry when the database is started.
            case 1105:  // Could not allocate space for object '%.*ls'%.*ls in database '%.*ls' because the '%.*ls' filegroup is full. Create disk space by deleting unneeded files, dropping objects
                // in the filegroup, adding additional files to the filegroup, or setting autogrowth on
            case 1456: // The ALTER DATABASE command could not be sent to the remote server instance '%.*ls'. The database mirroring configuration was not changed. Verify that the server is
                // connected, and try again.
            case 5061: // ALTER DATABASE failed because a lock could not be placed on database '%.*ls'. Try again later.
            case 10930: // The service is currently too busy. Please try again later.
            case 45168: // The server '%.*ls' has too many active connections.  Try again later.
            case 40642: // The server is currently too busy.  Please try again later.
            case 40675: // The service is currently too busy.  Please try again later.
            case 40825: // Unable to complete request now. Please try again later.
                return new MssqlTransientResourceException(createErrorDetails(token));
        }

        if (token.getClassification() == GENERAL_ERROR && token.getNumber() == 4002) {
            return new ProtocolException(token.getMessage());
        }

        switch (token.getClassification()) {
            case OBJECT_DOES_NOT_EXIST:
            case SYNTAX_ERROR:
                return new MssqlBadGrammarException(createErrorDetails(token), sql);
            case INCONSISTENT_NO_LOCK:
                return new MssqlDataIntegrityViolationException(createErrorDetails(token));
            case TX_DEADLOCK:
                return new MssqlRollbackException(createErrorDetails(token));
            case SECURITY:
                return new MssqlPermissionDeniedException(createErrorDetails(token));
            case GENERAL_ERROR:
                return new MssqlNonTransientException(createErrorDetails(token));
            case OUT_OF_RESOURCES:
                return new MssqlTransientResourceException(createErrorDetails(token));
            default:
                return new MssqlNonTransientResourceException(createErrorDetails(token));
        }
    }

    /**
     * Handle {@link Message}s and inspect for {@link ErrorToken} to emit a {@link R2dbcException}.
     *
     * @param message the message.
     * @param sink    the outbound sink.
     */
    void handleErrorResponse(Message message, SynchronousSink<Message> sink) {

        if (message instanceof ErrorToken) {
            sink.error(createException((ErrorToken) message, this.sql));
        } else {
            sink.next(message);
        }
    }

    /**
     * Create a {@link R2dbcException} for a {@link ErrorToken}.
     *
     * @param message the message.
     */
    RuntimeException createException(ErrorToken message) {
        return createException(message, this.sql);
    }

    static ErrorDetails createErrorDetails(AbstractInfoToken token) {
        return new ErrorDetails(token.getMessage(), token.getNumber(), token.getState(), token.getInfoClass(), token.getServerName(), token.getProcName(), token.getLineNumber());
    }

    /**
     * SQL Server-specific {@link R2dbcBadGrammarException}.
     */
    static final class MssqlBadGrammarException extends R2dbcBadGrammarException implements MssqlException {

        private final ErrorDetails errorDetails;

        MssqlBadGrammarException(ErrorDetails errorDetails, String offendingSql) {
            super(errorDetails.getMessage(), errorDetails.getStateCode(), (int) errorDetails.getNumber(), offendingSql);
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return errorDetails;
        }

    }

    /**
     * SQL Server-specific {@link R2dbcDataIntegrityViolationException}.
     */
    static final class MssqlDataIntegrityViolationException extends R2dbcDataIntegrityViolationException implements MssqlException {

        private final ErrorDetails errorDetails;

        MssqlDataIntegrityViolationException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getStateCode(), (int) errorDetails.getNumber());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return errorDetails;
        }

    }

    /**
     * SQL Server-specific {@link R2dbcNonTransientException}.
     */
    static final class MssqlNonTransientException extends R2dbcNonTransientException implements MssqlException {

        private final ErrorDetails errorDetails;

        MssqlNonTransientException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getStateCode(), (int) errorDetails.getNumber());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return errorDetails;
        }

    }

    /**
     * SQL Server-specific {@link R2dbcNonTransientResourceException}.
     */
    static final class MssqlNonTransientResourceException extends R2dbcNonTransientResourceException implements MssqlException {

        private final ErrorDetails errorDetails;

        MssqlNonTransientResourceException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getStateCode(), (int) errorDetails.getNumber());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return errorDetails;
        }

    }

    /**
     * SQL Server-specific {@link R2dbcPermissionDeniedException}.
     */
    static final class MssqlPermissionDeniedException extends R2dbcPermissionDeniedException implements MssqlException {

        private final ErrorDetails errorDetails;

        MssqlPermissionDeniedException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getStateCode(), (int) errorDetails.getNumber());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return errorDetails;
        }

    }

    /**
     * SQL Server-specific {@link R2dbcRollbackException}.
     */
    static final class MssqlRollbackException extends R2dbcRollbackException implements MssqlException {

        private final ErrorDetails errorDetails;

        MssqlRollbackException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getStateCode(), (int) errorDetails.getNumber());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return errorDetails;
        }

    }

    /**
     * SQL Server-specific {@link R2dbcTimeoutException}.
     */
    static final class MssqlTimeoutException extends R2dbcTimeoutException implements MssqlException {

        private final ErrorDetails errorDetails;

        MssqlTimeoutException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getStateCode(), (int) errorDetails.getNumber());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return errorDetails;
        }

    }

    /**
     * SQL Server-specific {@link R2dbcTransientException}.
     */
    static final class MssqlTransientException extends R2dbcTransientException implements MssqlException {

        private final ErrorDetails errorDetails;

        public MssqlTransientException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getStateCode(), (int) errorDetails.getNumber());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return errorDetails;
        }

    }

    /**
     * SQL Server-specific {@link R2dbcTransientResourceException}.
     */
    private static final class MssqlTransientResourceException extends R2dbcTransientResourceException implements MssqlException {

        private final ErrorDetails errorDetails;

        MssqlTransientResourceException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getStateCode(), (int) errorDetails.getNumber());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return errorDetails;
        }

    }

}
