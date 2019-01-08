/*
 * Copyright 2018-2019 the original author or authors.
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

package io.r2dbc.mssql;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Row;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Microsoft SQL Server-specific {@link Row} implementation.
 * A {@link Row} is stateful regarding its data state. It holds a {@link RowToken} along with row data that needs to be deallocated after processing the row. This row is no longer usable once it
 * was {@link #release() released}.
 *
 * @author Mark Paluch
 * @see #release()
 * @see ReferenceCounted
 */
final class MssqlRow extends ColumnSource implements Row {

    private static final AtomicIntegerFieldUpdater<MssqlRow> STATE_ACCESSOR = AtomicIntegerFieldUpdater.newUpdater(MssqlRow.class, "state");

    private static final int STATE_ACTIVE = 0;

    private static final int STATE_RELEASED = 1;

    private final Codecs codecs;

    private final RowToken rowToken;

    // see STATE_UPDATER
    @SuppressWarnings("unused")
    private volatile int state = STATE_ACTIVE;

    MssqlRow(Codecs codecs, List<Column> columns, Map<String, Column> nameKeyedColumns, RowToken rowToken) {

        super(columns, nameKeyedColumns);
        this.codecs = codecs;
        this.rowToken = rowToken;
    }

    /**
     * Create a new {@link MssqlRow}.
     *
     * @param codecs   the codecs to decode tabular data.
     * @param rowToken the row data.
     * @param columns  column specifications.
     * @return
     */
    static MssqlRow toRow(Codecs codecs, RowToken rowToken, ColumnMetadataToken columns) {

        Assert.requireNonNull(codecs, "Codecs must not be null");
        Assert.requireNonNull(rowToken, "RowToken must not be null");
        Assert.requireNonNull(columns, "ColMetadataToken must not be null");

        return new MssqlRow(codecs, columns.getColumns(), columns.toMap(), rowToken);
    }

    @Override
    @Nullable
    public <T> T get(Object identifier, Class<T> type) {

        Assert.requireNonNull(identifier, "Identifier must not be null");
        Assert.requireNonNull(type, "Type must not be null");
        requireNotReleased();

        Column column = super.getColumn(identifier);

        ByteBuf columnData = this.rowToken.getColumnData(column.getIndex());

        if (columnData == null) {
            return null;
        }

        columnData.markReaderIndex();

        try {
            return this.codecs.decode(columnData, column, type);
        } finally {
            columnData.resetReaderIndex();
        }
    }

    /**
     * Decrement the reference count and release the {@link RowToken} to allow deallocation of underlying memory.
     */
    public void release() {
        requireNotReleased();
        if (STATE_ACCESSOR.compareAndSet(this, STATE_ACTIVE, STATE_RELEASED)) {
            this.rowToken.release();
        }
    }

    private void requireNotReleased() {
        if (STATE_ACCESSOR.get(this) == STATE_RELEASED) {
            throw new IllegalStateException("Value cannot be retrieved after row has been released");
        }
    }
}
