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

package io.r2dbc.mssql.message.tds;

import io.r2dbc.mssql.AbstractMssqlException;
import reactor.util.annotation.Nullable;

/**
 * Exception indicating unsupported or invalid protocol states. This exception is thrown in cases where e.g. the clients
 * receives an invalid length, unexpected protocol frame or cannot decode a particular protocol frame. If a
 * {@link ProtocolException} is thrown, then the underlying transport connection is closed.
 *
 * @author Mark Paluch
 */
public final class ProtocolException extends AbstractMssqlException {

    /**
     * Creates a new exception.
     *
     * @param reason the reason for the error. Set as the exception's message and retrieved with {@link #getMessage()}.
     */
    public ProtocolException(@Nullable String reason) {
        super(reason, null, DRIVER_ERROR_NONE);
    }

    /**
     * Creates a new exception.
     *
     * @param reason          the reason for the error. Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param driverErrorCode the driver error code.
     */
    public ProtocolException(@Nullable String reason, int driverErrorCode) {
        super(reason, null, DRIVER_ERROR_NONE);
    }

    /**
     * Creates a new exception.
     *
     * @param reason the reason for the error. Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param cause  the cause
     */
    public ProtocolException(@Nullable String reason, @Nullable Throwable cause) {
        super(reason, null, DRIVER_ERROR_NONE, cause);
    }

    /**
     * Creates a new exception.
     *
     * @param cause           the cause
     * @param driverErrorCode the driver error code.
     */
    public ProtocolException(@Nullable Throwable cause, int driverErrorCode) {
        super(null, null, driverErrorCode, cause);
    }

    /**
     * Create a new {@link ProtocolException} for invalid TDS.
     *
     * @param reason the reason for the error. Set as the exception's message and retrieved with {@link #getMessage()}.
     * @return the {@link ProtocolException}
     */
    public static ProtocolException invalidTds(String reason) {
        return new ProtocolException(reason, DRIVER_ERROR_INVALID_TDS);
    }

    /**
     * Create a new {@link ProtocolException} for an unsupported configuration.
     *
     * @param reason the reason for the error. Set as the exception's message and retrieved with {@link #getMessage()}.
     * @return the {@link ProtocolException}
     */
    public static ProtocolException unsupported(String reason) {
        return new ProtocolException(reason, DRIVER_ERROR_UNSUPPORTED_CONFIG);
    }
}
