/*
 * Copyright 2018 the original author or authors.
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
package io.r2dbc.mssql.client;

import io.r2dbc.spi.R2dbcException;
import reactor.util.annotation.Nullable;

/**
 * Exception indicating unsupported or invalid protocol states. This exception is thrown in cases where e.g. the clients
 * receives an invalid length, unexpected protocol frame or cannot decode a particular protocol frame. If a
 * {@link ProtocolException} is thrown, then the underlying transport connection is closed.
 * 
 * @author Mark Paluch
 */
public final class ProtocolException extends R2dbcException {

	/**
	 * Creates a new exception.
	 *
	 * @param reason the reason for the error. Set as the exception's message and retrieved with {@link #getMessage()}.
	 */
	public ProtocolException(@Nullable String reason) {
		super(reason);
	}

	/**
	 * Creates a new exception.
	 *
	 * @param reason the reason for the error. Set as the exception's message and retrieved with {@link #getMessage()}.
	 * @param cause the cause
	 */
	public ProtocolException(@Nullable String reason, @Nullable Throwable cause) {
		super(reason, cause);
	}

	/**
	 * Creates a new exception.
	 *
	 * @param cause the cause
	 */
	public ProtocolException(@Nullable Throwable cause) {
		super(cause);
	}
}
