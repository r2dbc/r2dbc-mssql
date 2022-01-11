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

import java.util.ArrayList;
import java.util.List;

/**
 * Table name token. Used to send the table name to the client only when in browser mode or from cursors.
 *
 * @author Mark Paluch
 */
public class TabnameToken extends AbstractDataToken {

    public static final byte TYPE = (byte) 0xA4;

    private final List<Identifier> tableNames;

    private TabnameToken(List<Identifier> tableNames) {

        super(TYPE);
        this.tableNames = tableNames;
    }

    /**
     * Decode a {@link TabnameToken}.
     *
     * @param buffer the data buffer.
     * @return the {@link TabnameToken}.
     */
    public static TabnameToken decode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        int length = Decode.uShort(buffer);

        int readerIndex = buffer.readerIndex();

        List<Identifier> tableNames = new ArrayList<>();

        while (buffer.readerIndex() - readerIndex < length) {
            tableNames.add(Identifier.decode(buffer));
        }

        return new TabnameToken(tableNames);
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link TabnameToken}.
     *
     * @param buffer the data buffer.
     * @return {@code true} if the buffer contains sufficient data to entirely decode a {@link TabnameToken}.
     */
    public static boolean canDecode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        if (buffer.readableBytes() >= 5) {

            Integer requiredLength = Decode.peekUShort(buffer);
            return requiredLength != null && buffer.readableBytes() >= (requiredLength + /* length field */ 2);
        }

        return false;
    }

    public List<Identifier> getTableNames() {
        return this.tableNames;
    }

    @Override
    public String getName() {
        return "TABNAME";
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getName());
        sb.append(" [names=").append(this.tableNames);
        sb.append(']');
        return sb.toString();
    }

}
