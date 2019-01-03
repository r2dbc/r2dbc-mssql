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
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Column metadata token.
 *
 * @author Mark Paluch
 */
public final class ColumnMetadataToken extends AbstractDataToken {

    public static final byte TYPE = (byte) 0x81;

    private static final byte TYPE_SQLDATACLASSIFICATION = (byte) 0xa3;

    public static final int NO_COLUMNS = 0xFFFF;

    private final List<Column> columns;

    private final Map<String, Column> namedColumns;

    /**
     * Creates a new {@link ColumnMetadataToken}.
     *
     * @param type    token type.
     * @param columns the columns.
     */
    private ColumnMetadataToken(List<Column> columns) {

        super(TYPE);
        this.columns = Collections.unmodifiableList(Assert.requireNonNull(columns, "Columns must not be null"));

        Map<String, Column> byName = new LinkedHashMap<>(this.columns.size());

        for (Column column : columns) {
            byName.put(column.getName(), column);
        }

        this.namedColumns = Collections.unmodifiableMap(byName);
    }

    /**
     * Creates a new {@link ColumnMetadataToken} given {@link List} of {@link Column}s.
     *
     * @param columns the columns.
     * @return
     */
    public static ColumnMetadataToken create(List<Column> columns) {
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
        if (0xFFFF == columnCount) {
            return new ColumnMetadataToken(Collections.emptyList());
        }

        if (encryptionSupported) {

            int tableSize = Decode.uShort(buffer);

            if (tableSize != 0) {
                throw new UnsupportedOperationException("Driver does not support encryption");
            }
        }

        List<Column> columns = new ArrayList<>(columnCount);

        for (int i = 0; i < columnCount; i++) {

            Column column = decodeColumn(buffer, encryptionSupported, i);

            columns.add(column);

            if (buffer.readableBytes() > 1) {

                buffer.markReaderIndex();
                byte nextToken = Decode.asByte(buffer);
                buffer.resetReaderIndex();

                if (nextToken == TYPE_SQLDATACLASSIFICATION) {
                    throw new UnsupportedOperationException("Driver does not support SQL Data Classification");
                }
            }
        }

        return new ColumnMetadataToken(Collections.unmodifiableList(columns));
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

    public List<Column> getColumns() {
        return this.columns;
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
        sb.append(" [columns=").append(this.columns);
        sb.append(']');
        return sb.toString();
    }
}
