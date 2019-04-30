/*
 * Copyright 2018-2019 the original author or authors.
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

import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.RowMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Microsoft SQL Server-specific {@link RowMetadata}.
 *
 * @author Mark Paluch
 */
final class MssqlRowMetadata extends ColumnSource implements RowMetadata {

    private final Codecs codecs;

    private final Map<Column, MssqlColumnMetadata> metadataCache = new HashMap<>();

    private final CollatedCollection columnset;

    /**
     * Creates a new {@link MssqlColumnMetadata}.
     *
     * @param codecs           the codec registry.
     * @param columns          collection of {@link Column}s.
     * @param nameKeyedColumns name-keyed {@link Map} of {@link Column}s.
     */
    MssqlRowMetadata(Codecs codecs, List<Column> columns, Map<String, Column> nameKeyedColumns) {
        super(columns, getNameKeyedColumns(nameKeyedColumns));
        this.codecs = Assert.requireNonNull(codecs, "Codecs must not be null");

        List<String> orderedColumns = new ArrayList<>(columns.size());
        for (Column column : columns) {
            orderedColumns.add(column.getName());
        }

        this.columnset = new CollatedCollection(orderedColumns);
    }

    private static Map<String, Column> getNameKeyedColumns(Map<String, Column> nameKeyedColumns) {

        Map<String, Column> columns = new TreeMap<>(EscapeAwareComparator.INSTANCE);
        columns.putAll(nameKeyedColumns);

        return columns;
    }

    /**
     * Creates a new {@link MssqlColumnMetadata}.
     *
     * @param codecs         the codec registry.
     * @param columnMetadata the column metadata.
     */
    public static MssqlRowMetadata create(Codecs codecs, ColumnMetadataToken columnMetadata) {

        Assert.notNull(columnMetadata, "ColumnMetadata must not be null");

        return new MssqlRowMetadata(codecs, columnMetadata.getColumns(), columnMetadata.toMap());
    }

    @Override
    public MssqlColumnMetadata getColumnMetadata(Object identifier) {
        return this.metadataCache.computeIfAbsent(this.getColumn(identifier), column -> new MssqlColumnMetadata(column, codecs));
    }

    @Override
    public List<MssqlColumnMetadata> getColumnMetadatas() {

        List<MssqlColumnMetadata> metadatas = new ArrayList<>(this.getColumnCount());

        for (int i = 0; i < this.getColumnCount(); i++) {

            MssqlColumnMetadata columnMetadata = this.metadataCache.computeIfAbsent(this.getColumn(i), column -> new MssqlColumnMetadata(column, codecs));
            metadatas.add(columnMetadata);
        }

        return metadatas;
    }

    @Override
    public Collection<String> getColumnNames() {
        return this.columnset;
    }
}
