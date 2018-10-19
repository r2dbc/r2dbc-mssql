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
package io.r2dbc.mssql.message.header;

import java.util.EnumSet;
import java.util.Set;

/**
 * Packet header status as defined in ch {@literal 2.2.3.1.2 Status} of the TDS v20180912 spec.
 * <p/>
 * Status is a bit field used to indicate the message state. Status is a 1-byte unsigned char. The following Status bit
 * flags are defined.
 */
public enum Status {

	NORMAL(0x00), EOM(0x01), IGNORE(0x02),

	/**
	 * RESETCONNECTION
	 * 
	 * @since TDS 7.1
	 */
	RESET_CONNECTION(0x08),

	/**
	 * RESETCONNECTIONSKIPTRAN
	 * 
	 * @since TDS 7.3
	 */
	RESET_CONNECTION_SKIP_TRAN(0x10);

	Status(int bits) {
		this.bits = Integer.valueOf(bits).byteValue();
	}

	private final byte bits;

	/**
	 * Create {@link Status} {@link Set} from the given {@code bitmask}.
	 * 
	 * @param bitmask
	 * @return
	 */
	public static Set<Status> fromBitmask(byte bitmask) {

		EnumSet<Status> result = EnumSet.noneOf(Status.class);

		for (Status status : values()) {
			if ((bitmask & status.getBits()) != 0) {
				result.add(status);
			}
		}

		return result;
	}

	public int getBits() {
		return this.bits;
	}
}
