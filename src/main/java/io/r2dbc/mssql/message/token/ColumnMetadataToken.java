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
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Column metadata token.
 *
 * @author Mark Paluch
 */
public final class ColumnMetadataToken extends AbstractDataToken {

    public static final byte TYPE = (byte) 0x81;

    public static final int NO_COLUMNS = 0xFFFF;

    private static final byte TYPE_SQLDATACLASSIFICATION = (byte) 0xa3;

    private static final ColumnMetadataToken EMPTY = new ColumnMetadataToken(new Column[0]);

    private final Column[] columns;

    private final Map<String, Column> namedColumns;

    /**
     * Creates a new {@link ColumnMetadataToken}.
     *
     * @param columns the columns.
     */
    private ColumnMetadataToken(Column[] columns) {

        super(TYPE);

        this.columns = columns;

        if (columns.length == 1) {
            this.namedColumns = Collections.singletonMap(columns[0].getName(), columns[0]);
        } else {

            Map<String, Column> byName = new HashMap<>(this.columns.length, 1);

            for (Column column : columns) {
                Column old = byName.put(column.getName(), column);
                if (old != null) {
                    byName.put(column.getName(), old);
                }
            }

            this.namedColumns = byName;
        }
    }

    /**
     * Creates a new {@link ColumnMetadataToken} given {@link List} of {@link Column}s.
     *
     * @param columns the columns.
     * @return the {@link ColumnMetadataToken}.
     */
    public static ColumnMetadataToken create(Column[] columns) {
        return new ColumnMetadataToken(columns);
    }

    /**
     * Decode the {@link ColumnMetadataToken} response from a {@link ByteBuf}.
     *
     * @param buffer              must not be null.
     * @param encryptionSupported whether encryption is supported.
     * @return the decoded {@link ColumnMetadataToken}.
     */
    public static ColumnMetadataToken decode(ByteBuf buffer, boolean encryptionSupported) {

        int columnCount = Decode.uShort(buffer);

        // Handle the magic NoMetaData value
        if (columnCount == NO_COLUMNS) {
            return EMPTY;
        }

        if (encryptionSupported) {

            int tableSize = Decode.uShort(buffer);

            if (tableSize != 0) {
                throw new UnsupportedOperationException("Driver does not support encryption");
            }
        }

        Column[] columns = new Column[columnCount];

        for (int i = 0; i < columnCount; i++) {

            columns[i] = decodeColumn(buffer, encryptionSupported, i);
            decodeDataClassification(buffer);
        }

        return new ColumnMetadataToken(columns);
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link ColumnMetadataToken}.
     *
     * @param buffer              the data buffer.
     * @param encryptionSupported whether encryption is supported.
     * @return {@code true} if the buffer contains sufficient data to entirely decode a {@link ColumnMetadataToken}.
     */
    public static boolean canDecode(ByteBuf buffer, boolean encryptionSupported) {

        if (buffer.readableBytes() < 2) {
            return false;
        }

        int readerIndex = buffer.readerIndex();

        try {
            int columnCount = Decode.uShort(buffer);

            // Handle the magic NoMetaData value
            if (columnCount == NO_COLUMNS) {
                return true;
            }

            if (encryptionSupported) {

                if (buffer.readableBytes() < 2) {
                    return false;
                }
                buffer.skipBytes(2);
            }

            for (int i = 0; i < columnCount; i++) {

                if (!TypeInformation.canDecode(buffer, true)) {
                    return false;
                }

                if (!canDecodeColumn(buffer, encryptionSupported)) {
                    return false;
                }
            }
        } finally {
            buffer.readerIndex(readerIndex);
        }

        return true;
    }

    private static boolean canDecodeColumn(ByteBuf buffer, boolean encryptionSupported) {

        TypeInformation typeInfo = TypeInformation.decode(buffer, true);

        if (typeInfo.getServerType() == SqlServerType.TEXT || typeInfo.getServerType() == SqlServerType.NTEXT
            || typeInfo.getServerType() == SqlServerType.IMAGE) {
            // Yukon and later, table names are returned as multi-part SQL identifiers.
            if (!Identifier.canDecodeAndSkipBytes(buffer)) {
                return false;
            }
        }

        if (encryptionSupported && typeInfo.isEncrypted()) {
            throw new UnsupportedOperationException("Driver does not support encryption");
        }

        if (!buffer.isReadable()) {
            return false;
        }

        int length = buffer.readByte() * 2;

        if (length > buffer.readableBytes()) {
            return false;
        }

        buffer.skipBytes(length);
        decodeDataClassification(buffer);

        return true;
    }

    private static void decodeDataClassification(ByteBuf buffer) {

        if (buffer.readableBytes() > 1) {

            buffer.markReaderIndex();
            byte nextToken = Decode.asByte(buffer);
            buffer.resetReaderIndex();

            if (nextToken == TYPE_SQLDATACLASSIFICATION) {
                throw new UnsupportedOperationException("Driver does not support SQL Data Classification");
            }
        }
    }

    private static Column decodeColumn(ByteBuf buffer, boolean encryptionSupported, int columnIndex) {

        TypeInformation typeInfo = TypeInformation.decode(buffer, true);
        Identifier tableName = null;

        if (typeInfo.getServerType() == SqlServerType.TEXT || typeInfo.getServerType() == SqlServerType.NTEXT
            || typeInfo.getServerType() == SqlServerType.IMAGE) {
            // Yukon and later, table names are returned as multi-part SQL identifiers.
            tableName = Identifier.decode(buffer);
        }

        if (encryptionSupported && typeInfo.isEncrypted()) {
            throw new UnsupportedOperationException("Driver does not support encryption");
        }

        String name = Decode.unicodeBString(buffer);

        return new Column(columnIndex, name, typeInfo, tableName);
    }

    public Column[] getColumns() {
        return this.columns;
    }

    public boolean hasColumns() {
        return this.columns.length != 0;
    }

    @Override
    public String getName() {
        return "COLMETADATA";
    }

    public Map<String, Column> toMap() {
        return this.namedColumns;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [columns=").append(Arrays.toString(this.columns));
        sb.append(']');
        return sb.toString();
    }

}
