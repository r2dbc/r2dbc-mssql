/*
 * Copyright 2018-2020 the original author or authors.
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

import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Object that provides access to {@link Column}s by index and by name. Column index starts at {@literal 0} (zero-based index).
 *
 * @author Mark Paluch
 */
abstract class ColumnSource {

    private final Column[] columns;

    private final Map<String, Column> nameKeyedColumns;

    ColumnSource(Column[] columns, Map<String, Column> nameKeyedColumns) {

        this.columns = columns;
        this.nameKeyedColumns = nameKeyedColumns;
    }

    /**
     * Lookup {@link Column} by {@link Column#getIndex() index} or by its {@link Column#getName() name}.
     *
     * @param identifier the index or name.
     * @return the column.
     * @throws IllegalArgumentException if the column cannot be retrieved.
     * @throws IllegalArgumentException when {@code identifier} is {@code null}.
     */
    Column getColumn(Object identifier) {

        Assert.requireNonNull(identifier, "Identifier must not be null");

        if (identifier instanceof Integer) {
            return getColumn((int) identifier);
        }

        if (identifier instanceof String) {
            return getColumn((String) identifier);
        }

        throw new IllegalArgumentException(String.format("Identifier [%s] is not a valid identifier. Should either be an Integer index or a String column name.", identifier));
    }

    /**
     * Lookup {@link Column} by its {@code index}.
     *
     * @param index the column index. Must be greater zero and less than {@link #getColumnCount()}.
     * @return the {@link Column}.
     */
    Column getColumn(int index) {

        if (this.columns.length > index && index >= 0) {
            return this.columns[index];
        }

        throw new IllegalArgumentException(String.format("Column index [%d] is larger than the number of columns [%d]", index, this.columns.length));
    }

    /**
     * Lookup {@link Column} by its {@code name}.
     *
     * @param name the column name.
     * @return the {@link Column}.
     */
    Column getColumn(String name) {

        Column column = findColumn(name);

        if (column == null) {
            throw new IllegalArgumentException(String.format("Column name [%s] does not exist in column names %s", name, this.nameKeyedColumns.keySet()));
        }

        return column;
    }

    /**
     * Lookup {@link Column} by its {@code name}.
     *
     * @param name the column name.
     * @return the {@link Column}.
     */
    @Nullable
    Column findColumn(String name) {

        Column column = this.nameKeyedColumns.get(name);

        if (column == null) {
            name = EscapeAwareColumnMatcher.findColumn(name, this.nameKeyedColumns.keySet());
            if (name != null) {
                column = this.nameKeyedColumns.get(name);
            }
        }

        return column;
    }

    /**
     * Returns the number of {@link Column}s.
     *
     * @return the number of {@link Column}s.
     */
    int getColumnCount() {
        return this.columns.length;
    }

    Column[] getColumns() {
        return this.columns;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [").append(Arrays.stream(this.columns).map(Column::getName).collect(Collectors.joining(", "))).append("]");
        return sb.toString();
    }
}
