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

package io.r2dbc.mssql;

import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.RowMetadata;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Microsoft SQL Server-specific {@link RowMetadata}.
 *
 * @author Mark Paluch
 */
final class MssqlRowMetadata extends NamedCollectionSupport<Column> implements RowMetadata, Collection<String> {

    private final Codecs codecs;

    @Nullable
    private Map<Column, MssqlColumnMetadata> metadataCache;

    /**
     * Creates a new {@link MssqlColumnMetadata}.
     *
     * @param codecs           the codec registry.
     * @param columns          collection of {@link Column}s.
     * @param nameKeyedColumns name-keyed {@link Map} of {@link Column}s.
     */
    MssqlRowMetadata(Codecs codecs, Column[] columns, Map<String, Column> nameKeyedColumns) {
        super(columns, nameKeyedColumns, Column::getName, "column");
        this.codecs = Assert.requireNonNull(codecs, "Codecs must not be null");
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
    public MssqlColumnMetadata getColumnMetadata(int index) {
        if (this.metadataCache == null) {
            this.metadataCache = new HashMap<>();
        }
        return this.metadataCache.computeIfAbsent(this.get(index), column -> new MssqlColumnMetadata(column, this.codecs));
    }

    @Override
    public MssqlColumnMetadata getColumnMetadata(String identifier) {
        if (this.metadataCache == null) {
            this.metadataCache = new HashMap<>();
        }
        return this.metadataCache.computeIfAbsent(this.get(identifier), column -> new MssqlColumnMetadata(column, this.codecs));
    }

    @Override
    public List<MssqlColumnMetadata> getColumnMetadatas() {

        if (this.metadataCache == null) {
            this.metadataCache = new HashMap<>();
        }

        List<MssqlColumnMetadata> metadatas = new ArrayList<>(this.getCount());

        for (int i = 0; i < this.getCount(); i++) {

            MssqlColumnMetadata columnMetadata = this.metadataCache.computeIfAbsent(this.get(i), column -> new MssqlColumnMetadata(column, this.codecs));
            metadatas.add(columnMetadata);
        }

        return metadatas;
    }

    @Override
    public boolean contains(String columnName) {
        return find(columnName) != null;
    }

}
