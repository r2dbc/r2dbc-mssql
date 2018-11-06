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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.TdsDataType;
import reactor.util.annotation.Nullable;

/**
 * Utility methods to encode RPC parameters.
 *
 * @author Mark Paluch
 */
public final class RpcEncoding {

    private RpcEncoding() {
    }

    /**
     * Encode a string parameter as {@literal NVARCHAR} or {@literal NTEXT} (depending on the size).
     *  @param buffer    the data buffer.
     * @param name      optional parameter name.
     * @param direction RPC parameter direction (in/out)
     * @param collation parameter value encoding.
     * @param value     the parameter value, can be {@literal null}.
     */
    public static void encodeString(ByteBuf buffer, @Nullable String name, RpcDirection direction, Collation collation, @Nullable String value) {

        TdsDataType dataType = StringCodec.getDataType(direction, value);

        encodeHeader(buffer, name, direction, dataType);
        StringCodec.doEncode(buffer, direction, collation, value);
    }

    /**
     * Encode an integer parameter as {@literal INTn}.
     *  @param buffer    the data buffer.
     * @param name      optional parameter name.
     * @param direction RPC parameter direction (in/out)
     * @param value     the parameter value, can be {@literal null}.
     */
    public static void encodeInteger(ByteBuf buffer, @Nullable String name, RpcDirection direction, @Nullable Integer value) {

        encodeHeader(buffer, name, direction, TdsDataType.INTN);

        Encode.asByte(buffer, 4); // max-len

        if (value == null) {
            Encode.asByte(buffer, 0); // len of data bytes
        } else {
            Encode.asByte(buffer, 4); // len of data bytes
            Encode.asInt(buffer, value);
        }
    }

    public static void encodeHeader(ByteBuf buffer, @Nullable String name, RpcDirection direction, TdsDataType dataType) {

        if (name != null) {

            Encode.asByte(buffer, (name.length() + 1) * 2);

            char at = '@';
            writeChar(buffer, at);

            for (int i = 0; i < name.length(); i++) {
                char ch = name.charAt(i);
                writeChar(buffer, ch);
            }
        } else {
            Encode.asByte(buffer, 0);
        }

        Encode.asByte(buffer, direction == RpcDirection.OUT ? 1 : 0);
        Encode.asByte(buffer, dataType.getValue());
    }

    private static void writeChar(ByteBuf buffer, char ch) {

        buffer.writeByte((byte) (ch & 0xFF));
        buffer.writeByte((byte) ((ch >> 8) & 0xFF));
    }
}
