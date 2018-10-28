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

package io.r2dbc.mssql;

import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Object that provides access to {@link Column}s by index and by name. Column index starts at {@literal 0} (zero-based index).
 *
 * @author Mark Paluch
 */
abstract class ColumnSource {

    private final List<Column> columns;

    private final Map<String, Column> nameKeyedColumns;

    ColumnSource(List<Column> columns, Map<String, Column> nameKeyedColumns) {

        Objects.requireNonNull(columns, "Columns must not be null");
        Objects.requireNonNull(nameKeyedColumns, "Name-keyed columns must not be null");
        Assert.isTrue(columns.size() == nameKeyedColumns.size(), "The size of columns and name-keyed columns must be the same");

        this.columns = columns;
        this.nameKeyedColumns = nameKeyedColumns;
    }

    /**
     * Lookup {@link Column} by {@link Column#getIndex() index} or by its {@link Column#getName() name}.
     *
     * @param identifier the index or name.
     * @return the column.
     * @throws IllegalArgumentException if the column cannot be retrieved.
     */
    Column getColumn(Object identifier) {

        Objects.requireNonNull(identifier, "Identifier must not be null");

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

        if (this.columns.size() > index && index >= 0) {
            return this.columns.get(index);
        }

        throw new IllegalArgumentException(String.format("Column index [%d] is larger than the number of columns [%d]", index, this.columns.size()));
    }

    /**
     * Lookup {@link Column} by its {@code name}.
     *
     * @param name the column name.
     * @return the {@link Column}.
     */
    Column getColumn(String name) {

        Column column = this.nameKeyedColumns.get(name);

        if (column == null) {
            throw new IllegalArgumentException(String.format("Column name [%s] does not exist in column names [%s]", name, this.nameKeyedColumns.keySet()));
        }

        return column;
    }

    /**
     * Returns the number of {@link Column}s.
     *
     * @return the number of {@link Column}s.
     */
    int getColumnCount() {
        return this.columns.size();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [").append(this.columns.stream().map(Column::getName).collect(Collectors.joining(", "))).append("]");
        return sb.toString();
    }
}
