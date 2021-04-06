/*
 * Copyright 2018-2021 the original author or authors.
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

/**
 * Error token.
 *
 * @author Mark Paluch
 */
public final class ErrorToken extends AbstractInfoToken {

    public static final byte TYPE = (byte) 0xAA;

    public ErrorToken(long length, long number, byte state, byte infoClass, String message, String serverName, String procName, long lineNumber) {
        super(TYPE, length, number, state, infoClass, message, serverName, procName, lineNumber);
    }

    /**
     * Decode the {@link ErrorToken}.
     *
     * @param buffer the data buffer.
     * @return the decoded {@link ErrorToken}
     */
    public static ErrorToken decode(ByteBuf buffer) {

        int length = Decode.uShort(buffer);
        long number = Decode.asLong(buffer);
        byte state = Decode.asByte(buffer);
        byte infoClass = Decode.asByte(buffer);

        String msgText = Decode.unicodeUString(buffer);
        String serverName = Decode.unicodeBString(buffer);
        String procName = Decode.unicodeBString(buffer);

        long lineNumber = buffer.readUnsignedInt();

        return new ErrorToken(length, number, state, infoClass, msgText, serverName, procName, lineNumber);
    }

    @Override
    public String getName() {
        return "ERROR";
    }

}
