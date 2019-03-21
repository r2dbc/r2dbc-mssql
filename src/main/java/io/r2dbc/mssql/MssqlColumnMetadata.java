/*
 * Copyright 2018 the original author or authors.
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
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.ColumnMetadata;

import java.util.Optional;

/**
 * Microsoft SQL Server-specific {@link ColumnMetadata} based on {@link Column}.
 *
 * @author Mark Paluch
 */
public final class MssqlColumnMetadata implements ColumnMetadata {

    private final Column column;

    /**
     * Creates a new {@link MssqlColumnMetadata}.
     *
     * @param column the column.
     */
    MssqlColumnMetadata(Column column) {
        this.column = Assert.requireNonNull(column, "Column must not be null");
    }

    @Override
    public String getName() {
        return this.column.getName();
    }

    @Override
    public Optional<Integer> getPrecision() {
        return Optional.of(this.column.getType().getPrecision());
    }

    @Override
    public Integer getType() {
        return this.column.getType().getServerType().ordinal();
    }

    /**
     * Returns the underlying {@link TypeInformation}.
     *
     * @return the {@link TypeInformation}.
     */
    public TypeInformation getTypeInformation() {
        return this.column.getType();
    }
}
