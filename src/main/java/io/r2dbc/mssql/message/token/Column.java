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

import io.r2dbc.mssql.codec.Decodable;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.Objects;

/**
 * A {@link Decodable} column within a result set.
 *
 * @author Mark Paluch
 */
public class Column implements Decodable {

    private final int index;

    private final String name;

    private final TypeInformation type;

    @Nullable
    private final Identifier table;

    /**
     * Creates a new {@link Column}.
     *
     * @param index the index (ordinal position) within the result set, zero-based.
     * @param name  the column name.
     * @param type  the associated type of this column.
     */
    public Column(int index, String name, TypeInformation type) {
        this(index, name, type, null);
    }

    /**
     * Creates a new {@link Column}.
     *
     * @param index the index (ordinal position) within the result set, zero-based.
     * @param name  the column name.
     * @param type  the associated type of this column.
     * @param table the optional {@link Identifier table name}.
     */
    public Column(int index, String name, TypeInformation type, @Nullable Identifier table) {

        this.index = index;
        this.name = Assert.requireNonNull(name, "Column name must not be null");
        this.type = Assert.requireNonNull(type, "Type information must not be null");
        this.table = table;
    }

    /**
     * Returns the column index.
     *
     * @return the column index.
     */
    public int getIndex() {
        return this.index;
    }

    /**
     * Returns the column name.
     *
     * @return the column name.
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * Returns the column {@link TypeInformation type}.
     *
     * @return the column {@link TypeInformation type}.
     */
    @Override
    public TypeInformation getType() {
        return this.type;
    }

    /**
     * Returns the {@link Identifier table} name.
     *
     * @return the {@link Identifier table} name, can be {@code null}.
     */
    @Nullable
    public Identifier getTable() {
        return this.table;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [name='").append(this.name).append('\"');
        sb.append(", type=").append(this.type);
        sb.append(", table=").append(this.table);
        sb.append(']');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Column)) {
            return false;
        }
        Column column = (Column) o;
        return this.index == column.index &&
            Objects.equals(this.name, column.name) &&
            Objects.equals(this.table, column.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.index, this.name, this.table);
    }

}
