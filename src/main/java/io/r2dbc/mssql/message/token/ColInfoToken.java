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
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Column info token. Describes the column information in browse mode.
 *
 * @author Mark Paluch
 */
public class ColInfoToken extends AbstractDataToken {

    public static final byte TYPE = (byte) 0xA5;

    public static final int STATUS_EXPRESSION = 0x04;

    public static final int STATUS_KEY = 0x08;

    public static final int STATUS_HIDDEN = 0x10;

    public static final int STATUS_DIFFERENT_NAME = 0x20;

    private static final ColInfoToken EMPTY = new ColInfoToken(Collections.emptyList());

    private final List<ColInfo> columns;

    private ColInfoToken(List<ColInfo> columns) {
        super(TYPE);

        this.columns = columns;
    }

    /**
     * Decode a {@link ColInfoToken}.
     *
     * @param buffer the data buffer.
     * @return the {@link ColInfoToken}.
     */
    public static ColInfoToken skip(ByteBuf buffer) {

        int length = Decode.uShort(buffer);
        buffer.skipBytes(length);

        return EMPTY;
    }

    /**
     * Decode a {@link ColInfoToken}.
     *
     * @param buffer the data buffer.
     * @return the {@link ColInfoToken}.
     */
    public static ColInfoToken decode(ByteBuf buffer) {

        int length = Decode.uShort(buffer);

        int readerIndex = buffer.readerIndex();

        List<ColInfo> columns = new ArrayList<>();

        while (buffer.readerIndex() - readerIndex < length) {

            byte column = Decode.asByte(buffer);
            byte table = Decode.asByte(buffer);
            byte status = Decode.asByte(buffer);
            String columnName = (status & STATUS_DIFFERENT_NAME) != 0 ? Decode.unicodeBString(buffer) : null;

            columns.add(new ColInfo(column, table, status, columnName));
        }

        return new ColInfoToken(columns);
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link ColInfoToken}.
     *
     * @param buffer the data buffer.
     * @return {@code true} if the buffer contains sufficient data to entirely decode a {@link ColInfoToken}.
     */
    public static boolean canDecode(ByteBuf buffer) {

        if (buffer.readableBytes() >= 5) {

            Integer requiredLength = Decode.peekUShort(buffer);
            return requiredLength != null && buffer.readableBytes() >= (requiredLength + /* length field */ 2);
        }

        return false;
    }

    public List<ColInfo> getColumns() {
        return this.columns;
    }

    @Override
    public String getName() {
        return "COLINFO";
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [columns=").append(this.columns);
        sb.append(']');
        return sb.toString();
    }

    /**
     * Column info for a column returned by a sp_cursor operation.
     */
    static final class ColInfo {

        /**
         * The column number in the result set.
         */
        private final byte column;

        /**
         * The number of the base table that the column was derived from. The value is 0 if the value of Status is EXPRESSION.
         */
        private final byte table;

        /**
         * 0x4: EXPRESSION (the column was the result of an expression). 0x8: KEY (the column is part of a key for the associated table).
         * 0x10: HIDDEN (the column was not requested, but was added because it was part of a key for the associated table).
         * 0x20: DIFFERENT_NAME (the column name is different than the requested column name in the case of a column alias).
         */
        private final byte status;

        /**
         * The base column name. This only occurs if DIFFERENT_NAME is set in Status.
         */
        @Nullable
        private final String name;

        private ColInfo(byte column, byte table, byte status, @Nullable String name) {
            this.column = column;
            this.table = table;
            this.status = status;
            this.name = name;
        }

        public byte getColumn() {
            return this.column;
        }

        public byte getTable() {
            return this.table;
        }

        public byte getStatus() {
            return this.status;
        }

        @Nullable
        public String getName() {
            return this.name;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [column=").append(this.column);
            sb.append(", table=").append(this.table);
            sb.append(", status=").append(this.status);
            sb.append(", columnName=\"").append(this.name).append('\"');
            sb.append(']');
            return sb.toString();
        }

    }

}
