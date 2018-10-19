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
package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;

/**
 * Info token.
 * 
 * @author Mark Paluch
 */
public class InfoToken extends DataToken {

	public static final byte TYPE = (byte) 0xAB;

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

	public InfoToken(long length, long number, byte state, byte infoClass, String message, String serverName,
			String procName, long lineNumber) {

		super(TYPE);
		this.length = length;
		this.number = number;
		this.state = state;
		this.infoClass = infoClass;
		this.message = message;
		this.serverName = serverName;
		this.procName = procName;
		this.lineNumber = lineNumber;
	}

	/**
	 * Decode the {@link InfoToken}.
	 * 
	 * @param buffer
	 * @return
	 */
	public static InfoToken decode(ByteBuf buffer) {

		int length = Decode.uShort(buffer);
		long number = Decode.asLong(buffer);
		byte state = Decode.asByte(buffer);
		byte infoClass = Decode.asByte(buffer);

		String msgText = Decode.unicodeUString(buffer);
		String serverName = Decode.unicodeBString(buffer);
		String procName = Decode.unicodeBString(buffer);

		long lineNumber = buffer.readUnsignedInt();

		return new InfoToken(length, number, state, infoClass, msgText, serverName, procName, lineNumber);
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
	public String getName() {
		return "INFO";
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer();
		sb.append(getClass().getSimpleName());
		sb.append(" [number=").append(this.number);
		sb.append(", state=").append(this.state);
		sb.append(", infoClass=").append(this.infoClass);
		sb.append(", message='").append(this.message).append('\'');
		sb.append(", serverName='").append(this.serverName).append('\'');
		sb.append(", procName='").append(this.procName).append('\'');
		sb.append(", lineNumber=").append(this.lineNumber);
		sb.append(']');
		return sb.toString();
	}
}
