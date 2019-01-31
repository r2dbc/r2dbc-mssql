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

import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.RowMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Microsoft SQL Server-specific {@link RowMetadata}.
 *
 * @author Mark Paluch
 */
final class MssqlRowMetadata implements RowMetadata {

    private final ColumnSource columnSource;

    private final Codecs codecs;

    private final Map<Column, MssqlColumnMetadata> metadataCache = new HashMap<>();

    /**
     * Creates a new {@link MssqlColumnMetadata}.
     *
     * @param columnSource the source of {@link Column}s.
     * @param codecs       the codec registry.
     */
    MssqlRowMetadata(ColumnSource columnSource, Codecs codecs) {
        this.columnSource = Assert.requireNonNull(columnSource, "ColumnSource must not be null");
        this.codecs = Assert.requireNonNull(codecs, "Codecs must not be null");
    }

    @Override
    public MssqlColumnMetadata getColumnMetadata(Object identifier) {
        return this.metadataCache.computeIfAbsent(this.columnSource.getColumn(identifier), column -> new MssqlColumnMetadata(column, codecs));
    }

    @Override
    public List<MssqlColumnMetadata> getColumnMetadatas() {

        List<MssqlColumnMetadata> metadatas = new ArrayList<>(this.columnSource.getColumnCount());

        for (int i = 0; i < this.columnSource.getColumnCount(); i++) {

            MssqlColumnMetadata columnMetadata = this.metadataCache.computeIfAbsent(this.columnSource.getColumn(i), column -> new MssqlColumnMetadata(column, codecs));
            metadatas.add(columnMetadata);
        }

        return metadatas;
    }
}
