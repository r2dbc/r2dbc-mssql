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

package io.r2dbc.mssql.message.type;

import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

/**
 * Enumeration of SQL server data types.
 */
public enum SqlServerType {

    // @formatter:off
    UNKNOWN(Category.UNKNOWN,           "unknown"),
    TINYINT(Category.NUMERIC,           "tinyint",          1, TdsDataType.INTN, TdsDataType.INT1),
    BIT(Category.NUMERIC,               "bit",              1, TdsDataType.INTN, TdsDataType.INT1),
    SMALLINT(Category.NUMERIC,          "smallint",         2, TdsDataType.INTN, TdsDataType.INT2),
    INTEGER(Category.NUMERIC,           "int",              4, TdsDataType.INTN, TdsDataType.INT4),
    BIGINT(Category.NUMERIC,            "bigint",           8, TdsDataType.INTN, TdsDataType.INT8),
    FLOAT(Category.NUMERIC,             "float",            8, TdsDataType.FLOATN, TdsDataType.FLOAT8),
    REAL(Category.NUMERIC,              "real",             4, TdsDataType.FLOATN, TdsDataType.FLOAT4),
    SMALLDATETIME(Category.DATETIME,    "smalldatetime",    4, TdsDataType.DATETIMEN, TdsDataType.DATETIME4),
    DATETIME(Category.DATETIME,         "datetime",         8, TdsDataType.DATETIMEN, TdsDataType.DATETIME8),
    DATE(Category.DATE,                 "date",             3, TdsDataType.DATEN),
    TIME(Category.TIME,                 "time",             7, TdsDataType.TIMEN),
    DATETIME2(Category.DATETIME2,       "datetime2",        7, TdsDataType.DATETIME2N),
    DATETIMEOFFSET(Category.DATETIMEOFFSET, "datetimeoffset", 7, TdsDataType.DATETIMEOFFSETN),
    SMALLMONEY(Category.NUMERIC,        "smallmoney",       4, TdsDataType.MONEYN, TdsDataType.MONEY4),
    MONEY(Category.NUMERIC,             "money",            8, TdsDataType.MONEYN, TdsDataType.MONEY8),
    CHAR(Category.CHARACTER,            "char"),
    VARCHAR(Category.CHARACTER,         "varchar"),
    VARCHARMAX(Category.LONG_CHARACTER, "varchar", TdsDataType.BIGVARCHAR),
    TEXT(Category.LONG_CHARACTER,       "text",                 TdsDataType.TEXT),
    NCHAR(Category.NCHARACTER,          "nchar"),
    NVARCHAR(Category.NCHARACTER,       "nvarchar",         4000, TdsDataType.NVARCHAR),
    NVARCHARMAX(Category.LONG_NCHARACTER, "nvarchar", TdsDataType.NVARCHAR),
    NTEXT(Category.LONG_NCHARACTER,     "ntext", TdsDataType.NTEXT),
    BINARY(Category.BINARY,             "binary"),
    VARBINARY(Category.BINARY, "varbinary", 8000, TdsDataType.BIGVARBINARY),
    VARBINARYMAX(Category.LONG_BINARY, "varbinary", TdsDataType.BIGVARBINARY),
    IMAGE(Category.LONG_BINARY,         "image", TdsDataType.IMAGE),
    DECIMAL(Category.NUMERIC,           "decimal",          38, TdsDataType.DECIMALN),
    NUMERIC(Category.NUMERIC,           "numeric",          38, TdsDataType.NUMERICN),
    GUID(Category.GUID,                 "uniqueidentifier", 16, TdsDataType.GUID),
    SQL_VARIANT(Category.SQL_VARIANT,   "sql_variant", TdsDataType.SQL_VARIANT),
    UDT(Category.UDT,                   "udt"),
    XML(Category.XML,                   "xml"),
    TIMESTAMP(Category.TIMESTAMP,       "timestamp", 8, TdsDataType.BIGBINARY),
    GEOMETRY(Category.UDT,              "geometry"),
    GEOGRAPHY(Category.UDT,             "geography");
    // @formatter:on

    private final Category category;

    private final String name;

    private final int maxLength;

    @Nullable
    private final TdsDataType nullableType;

    private final TdsDataType fixedTypes[];

    /**
     * @param category     type category.
     * @param name         SQL server type name.
     * @param nullableType the nullable {@link TdsDataType}.
     * @param fixedTypes   zero or many fixed-length {@link TdsDataType}s.
     */
    SqlServerType(Category category, String name, TdsDataType nullableType, TdsDataType... fixedTypes) {
        this(category, name, 0, nullableType, fixedTypes);
    }

    /**
     * @param category     type category.
     * @param name         SQL server type name.
     * @param maxLength    maximal type length.
     * @param nullableType the nullable {@link TdsDataType}.
     * @param fixedTypes   zero or many fixed-length {@link TdsDataType}s.
     */
    SqlServerType(Category category, String name, int maxLength, TdsDataType nullableType, TdsDataType... fixedTypes) {

        Assert.isTrue(nullableType.getLengthStrategy() != LengthStrategy.FIXEDLENTYPE, String.format("Type [%s] specified a fixed-length strategy in its nullable type", name));

        for (TdsDataType fixedType : fixedTypes) {
            Assert.isTrue(fixedType.getLengthStrategy() == LengthStrategy.FIXEDLENTYPE, String.format("Type [%s] specified [%s] in its fixed length type [%s] ", name,
                fixedType.getLengthStrategy(), fixedType));
        }

        this.category = category;
        this.name = name;
        this.maxLength = maxLength;
        this.nullableType = nullableType;
        this.fixedTypes = fixedTypes;
    }

    SqlServerType(Category category, String name) {

        this.category = category;
        this.name = name;
        this.maxLength = 0;
        this.nullableType = null;
        this.fixedTypes = new TdsDataType[0];
    }

    /**
     * Resolve a {@link SqlServerType} by its {@code typeName}. Name comparison is case-insensitive.
     *
     * @param typeName the type name.
     * @return the resolved {@link SqlServerType}.
     * @throws IllegalArgumentException if the type name cannot be resolved to a {@link SqlServerType}
     */
    static SqlServerType of(String typeName) {

        for (SqlServerType type : SqlServerType.values())
            if (type.name.equalsIgnoreCase(typeName)) {
                return type;
            }

        throw new IllegalArgumentException(String.format("Unknown type: %s", typeName));
    }

    public int getMaxLength() {
        return this.maxLength;
    }

    @Nullable
    public TdsDataType getNullableType() {
        return this.nullableType;
    }

    public TdsDataType[] getFixedTypes() {
        return this.fixedTypes;
    }

    /**
     * Returns the type name.
     *
     * @return the type name.
     */
    @Override
    public String toString() {
        return this.name;
    }

    /**
     * Type categories.
     */
    enum Category {
        // @formatter:off
        BINARY,
        CHARACTER,
        DATE,
        DATETIME,
        DATETIME2,
        DATETIMEOFFSET,
        GUID,
        LONG_BINARY,
        LONG_CHARACTER,
        LONG_NCHARACTER,
        NCHARACTER,
        NUMERIC,
        UNKNOWN,
        TIME,
        TIMESTAMP,
        UDT,
        SQL_VARIANT,
        XML
        // @formatter:on
    }
}
