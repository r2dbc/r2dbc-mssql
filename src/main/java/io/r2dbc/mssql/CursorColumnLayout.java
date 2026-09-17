/*
 * Copyright 2026 the original author or authors.
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

package io.r2dbc.mssql;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.token.ColInfoToken;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.List;

/**
 * Column layout of a server-cursor result. Server cursor fetches append a row status column ({@code ROWSTAT}) to each row. The server marks that column as hidden
 * expression column through {@link ColInfoToken COLINFO}. A layout is only created for a verified row status column so that a user column that happens to be named
 * {@code ROWSTAT} is never hidden and rows are never suppressed based on user data.
 *
 * @author dom54
 * @see ColInfoToken
 */
final class CursorColumnLayout implements Message {

    /**
     * Name of the row status column synthesized by the server.
     */
    static final String ROW_STATUS_COLUMN_NAME = "ROWSTAT";

    /**
     * Row status for a row that was deleted after opening the cursor (keyset hole). Data columns do not contain values of the originally fetched row.
     */
    static final int ROW_STATUS_MISSING = 2;

    private final ColumnMetadataToken metadata;

    private final int rowStatusIndex;

    private CursorColumnLayout(ColumnMetadataToken metadata, int rowStatusIndex) {
        this.metadata = metadata;
        this.rowStatusIndex = rowStatusIndex;
    }

    /**
     * Create a {@link CursorColumnLayout} from {@link ColumnMetadataToken} and the {@link ColInfoToken} that describes the same columns. Returns {@code null} if the
     * trailing column is not a verified hidden row status column.
     *
     * @param metadata the column metadata.
     * @param colInfo  the column info.
     * @return the {@link CursorColumnLayout} or {@code null} if the result does not contain a verified hidden row status column.
     */
    @Nullable
    static CursorColumnLayout from(ColumnMetadataToken metadata, ColInfoToken colInfo) {

        Assert.requireNonNull(metadata, "ColumnMetadataToken must not be null");
        Assert.requireNonNull(colInfo, "ColInfoToken must not be null");

        Column[] columns = metadata.getColumns();
        List<ColInfoToken.ColInfo> infos = colInfo.getColumns();

        if (columns.length == 0 || infos.size() != columns.length) {
            return null;
        }

        int index = columns.length - 1;
        Column column = columns[index];
        ColInfoToken.ColInfo info = infos.get(index);

        // COLINFO column numbers are one-based and encoded as single byte
        if (Byte.toUnsignedInt(info.getColumn()) != ((index + 1) & 0xFF)) {
            return null;
        }

        // the row status column is not derived from a base table
        if (!info.isHidden() || !info.isExpression() || info.getTable() != 0) {
            return null;
        }

        TypeInformation type = column.getType();
        if (type.getServerType() != SqlServerType.INTEGER || !ROW_STATUS_COLUMN_NAME.equals(column.getName())) {
            return null;
        }

        return new CursorColumnLayout(metadata, index);
    }

    /**
     * @return the column metadata including the row status column.
     */
    ColumnMetadataToken getMetadata() {
        return this.metadata;
    }

    /**
     * @return the wire index of the row status column.
     */
    int getRowStatusIndex() {
        return this.rowStatusIndex;
    }

    /**
     * Returns whether the {@link RowToken} is a placeholder for a row that is missing (deleted since the cursor was opened).
     *
     * @param row the row.
     * @return {@code true} if the row status indicates a missing row.
     */
    boolean isMissing(RowToken row) {

        ByteBuf data = row.getColumnData(this.rowStatusIndex);

        if (data == null) {
            return false;
        }

        ByteBuf buffer = data.duplicate();
        Length length = Length.decode(buffer, this.metadata.getColumns()[this.rowStatusIndex].getType());

        if (length.isNull() || length.getLength() != 4 || buffer.readableBytes() < 4) {
            return false;
        }

        return buffer.readIntLE() == ROW_STATUS_MISSING;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [metadata=").append(this.metadata);
        sb.append(", rowStatusIndex=").append(this.rowStatusIndex);
        sb.append(']');
        return sb.toString();
    }

}
