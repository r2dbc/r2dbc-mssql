/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql;


import io.r2dbc.spi.R2dbcException;
import reactor.util.annotation.Nullable;

/**
 * Microsoft SQL Server-specific exception class.
 *
 * @author Mark Paluch
 */
public class AbstractMssqlException extends R2dbcException {

    public static final int DRIVER_ERROR_NONE = 0;

    public static final int DRIVER_ERROR_FROM_DATABASE = 2;

    public static final int DRIVER_ERROR_IO_FAILED = 3;

    public static final int DRIVER_ERROR_INVALID_TDS = 4;

    public static final int DRIVER_ERROR_SSL_FAILED = 5;

    public static final int DRIVER_ERROR_UNSUPPORTED_CONFIG = 6;

    public static final int DRIVER_ERROR_INTERMITTENT_TLS_FAILED = 7;

    public static final int ERROR_SOCKET_TIMEOUT = 8;

    public static final int ERROR_QUERY_TIMEOUT = 9;

    /**
     * Creates a new exception.
     */
    public AbstractMssqlException() {
        this((String) null);
    }

    /**
     * Creates a new exception.
     *
     * @param reason the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     */
    public AbstractMssqlException(@reactor.util.annotation.Nullable String reason) {
        this(reason, (String) null);
    }

    /**
     * Creates a new exception.
     *
     * @param reason   the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param sqlState the "SQLstate" string, which follows either the XOPEN SQLstate conventions or the SQL:2003 conventions
     */
    public AbstractMssqlException(@Nullable String reason, @Nullable String sqlState) {
        this(reason, sqlState, 0);
    }

    /**
     * Creates a new exception.
     *
     * @param reason    the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param sqlState  the "SQLstate" string, which follows either the XOPEN SQLstate conventions or the SQL:2003 conventions
     * @param errorCode a vendor-specific error code representing this failure
     */
    public AbstractMssqlException(@Nullable String reason, @Nullable String sqlState, int errorCode) {
        this(reason, sqlState, errorCode, null);
    }

    /**
     * Creates a new exception.
     *
     * @param reason    the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param sqlState  the "SQLstate" string, which follows either the XOPEN SQLstate conventions or the SQL:2003 conventions
     * @param errorCode a vendor-specific error code representing this failure
     * @param cause     the cause
     */
    public AbstractMssqlException(@Nullable String reason, @Nullable String sqlState, int errorCode, @Nullable Throwable cause) {
        super(reason, sqlState, errorCode, cause);
    }

    /**
     * Creates a new exception.
     *
     * @param reason   the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param sqlState the "SQLstate" string, which follows either the XOPEN SQLstate conventions or the SQL:2003 conventions
     * @param cause    the cause
     */
    public AbstractMssqlException(@Nullable String reason, @Nullable String sqlState, @Nullable Throwable cause) {
        this(reason, sqlState, 0, cause);
    }

    /**
     * Creates a new exception.
     *
     * @param reason the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param cause  the cause
     */
    public AbstractMssqlException(@Nullable String reason, @Nullable Throwable cause) {
        this(reason, null, cause);
    }

    /**
     * Creates a new exception.
     *
     * @param cause the cause
     */
    public AbstractMssqlException(@Nullable Throwable cause) {
        this(null, cause);
    }
}
