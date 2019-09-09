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
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Microsoft SQL Server-specific {@link RowMetadata}.
 *
 * @author Mark Paluch
 */
final class MssqlRowMetadata extends ColumnSource implements RowMetadata, Collection<String> {

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
        super(columns, nameKeyedColumns);
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
        return this.metadataCache.computeIfAbsent(this.getColumn(index), column -> new MssqlColumnMetadata(column, this.codecs));
    }

    @Override
    public MssqlColumnMetadata getColumnMetadata(String identifier) {
        if (this.metadataCache == null) {
            this.metadataCache = new HashMap<>();
        }
        return this.metadataCache.computeIfAbsent(this.getColumn(identifier), column -> new MssqlColumnMetadata(column, this.codecs));
    }

    @Override
    public List<MssqlColumnMetadata> getColumnMetadatas() {

        if (this.metadataCache == null) {
            this.metadataCache = new HashMap<>();
        }

        List<MssqlColumnMetadata> metadatas = new ArrayList<>(this.getColumnCount());

        for (int i = 0; i < this.getColumnCount(); i++) {

            MssqlColumnMetadata columnMetadata = this.metadataCache.computeIfAbsent(this.getColumn(i), column -> new MssqlColumnMetadata(column, this.codecs));
            metadatas.add(columnMetadata);
        }

        return metadatas;
    }

    @Override
    public Collection<String> getColumnNames() {
        return this;
    }

    @Override
    public int size() {
        return this.getColumnCount();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {

        if (o instanceof String) {
            return this.findColumn((String) o) != null;
        }

        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {

        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Iterator<String> iterator() {

        Column[] columns = this.getColumns();

        return new Iterator<String>() {

            int index = 0;

            @Override
            public boolean hasNext() {
                return columns.length > this.index;
            }

            @Override
            public String next() {
                Column column = columns[this.index++];
                return column.getName();
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        return (T[]) toArray();
    }

    @Override
    public Object[] toArray() {

        Object[] result = new Object[size()];

        for (int i = 0; i < size(); i++) {
            result[i] = this.getColumn(i).getName();
        }

        return result;
    }

    @Override
    public boolean add(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
