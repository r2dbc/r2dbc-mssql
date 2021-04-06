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

package io.r2dbc.mssql.message.type;

import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Type;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Enumeration of SQL server data types.
 */
public enum SqlServerType implements Type {

    // @formatter:off
    UNKNOWN(Category.UNKNOWN,               Object.class,  "unknown"),
    TINYINT(Category.NUMERIC,               Byte.class,    "tinyint",          1, TdsDataType.INTN, TdsDataType.INT1),
    BIT(Category.NUMERIC,                   Byte.class,    "bit",              1, TdsDataType.INTN, TdsDataType.INT1),
    SMALLINT(Category.NUMERIC,              Short.class,   "smallint",         2, TdsDataType.INTN, TdsDataType.INT2),
    INTEGER(Category.NUMERIC,               Integer.class, "int",              4, TdsDataType.INTN, TdsDataType.INT4),
    BIGINT(Category.NUMERIC,                Long.class,    "bigint",           8, TdsDataType.INTN, TdsDataType.INT8),
    FLOAT(Category.NUMERIC,                 Double.class,  "float",            8, TdsDataType.FLOATN, TdsDataType.FLOAT8),
    REAL(Category.NUMERIC,                  Float.class,   "real",             4, TdsDataType.FLOATN, TdsDataType.FLOAT4),
    SMALLDATETIME(Category.DATETIME,        LocalDateTime.class, "smalldatetime",    4, TdsDataType.DATETIMEN, TdsDataType.DATETIME4),
    DATETIME(Category.DATETIME,             LocalDateTime.class, "datetime",         8, TdsDataType.DATETIMEN, TdsDataType.DATETIME8),
    DATE(Category.DATE,                     LocalDate.class, "date",             3, TdsDataType.DATEN),
    TIME(Category.TIME,                     LocalTime.class, "time",             7, TdsDataType.TIMEN),
    DATETIME2(Category.DATETIME2,           LocalDateTime.class, "datetime2",        7, TdsDataType.DATETIME2N),
    DATETIMEOFFSET(Category.DATETIMEOFFSET, OffsetDateTime.class, "datetimeoffset", 7, TdsDataType.DATETIMEOFFSETN),
    SMALLMONEY(Category.NUMERIC,            BigDecimal.class, "smallmoney",       4, TdsDataType.MONEYN, TdsDataType.MONEY4),
    MONEY(Category.NUMERIC,                 BigDecimal.class, "money",            8, TdsDataType.MONEYN, TdsDataType.MONEY8),
    CHAR(Category.CHARACTER,                String.class,  "char"),
    VARCHAR(Category.CHARACTER,             String.class,  "varchar", 8000, TdsDataType.BIGVARCHAR),
    VARCHARMAX(Category.LONG_CHARACTER,     String.class,  "varchar", TdsDataType.BIGVARCHAR),
    TEXT(Category.LONG_CHARACTER,           Clob.class,    "text",                 TdsDataType.TEXT),
    NCHAR(Category.NCHARACTER,              String.class,  "nchar"),
    NVARCHAR(Category.NCHARACTER,           String.class,  "nvarchar",         4000, TdsDataType.NVARCHAR),
    NVARCHARMAX(Category.LONG_NCHARACTER,   String.class,  "nvarchar", TdsDataType.NVARCHAR),
    NTEXT(Category.LONG_NCHARACTER,         String.class,  "ntext", TdsDataType.NTEXT),
    BINARY(Category.BINARY,                 ByteBuffer.class,"binary"),
    VARBINARY(Category.BINARY,              ByteBuffer.class,"varbinary", 8000, TdsDataType.BIGVARBINARY),
    VARBINARYMAX(Category.LONG_BINARY,      ByteBuffer.class,"varbinary", TdsDataType.BIGVARBINARY),
    IMAGE(Category.LONG_BINARY,             ByteBuffer.class,"image", TdsDataType.IMAGE),
    DECIMAL(Category.NUMERIC,               BigDecimal.class,"decimal",          38, TdsDataType.DECIMALN),
    NUMERIC(Category.NUMERIC,               BigDecimal.class,"numeric",          38, TdsDataType.NUMERICN),
    GUID(Category.GUID,                     UUID.class,    "uniqueidentifier", 16, TdsDataType.GUID),
    SQL_VARIANT(Category.SQL_VARIANT,       Object.class,  "sql_variant", TdsDataType.SQL_VARIANT),
    UDT(Category.UDT,                       Object.class,  "udt"),
    XML(Category.XML,                       Object.class,  "xml"),
    TIMESTAMP(Category.TIMESTAMP,           byte[].class,  "timestamp", 8, TdsDataType.BIGBINARY),
    GEOMETRY(Category.UDT,                  Object.class,  "geometry"),
    GEOGRAPHY(Category.UDT,                 Object.class,  "geography");
    // @formatter:on

    private final Category category;

    private final Class<?> defaultJavaType;

    private final String name;

    private final int maxLength;

    @Nullable
    private final TdsDataType nullableType;

    private final TdsDataType[] fixedTypes;

    /**
     * @param category        type category.
     * @param defaultJavaType default Java type.
     * @param name            SQL server type name.
     * @param nullableType    the nullable {@link TdsDataType}.
     * @param fixedTypes      zero or many fixed-length {@link TdsDataType}s.
     */
    SqlServerType(Category category, Class<?> defaultJavaType, String name, TdsDataType nullableType, TdsDataType... fixedTypes) {
        this(category, defaultJavaType, name, 0, nullableType, fixedTypes);
    }

    /**
     * @param category        type category.
     * @param defaultJavaType default Java type.
     * @param name            SQL server type name.
     * @param maxLength       maximal type length.
     * @param nullableType    the nullable {@link TdsDataType}.
     * @param fixedTypes      zero or many fixed-length {@link TdsDataType}s.
     */
    SqlServerType(Category category, Class<?> defaultJavaType, String name, int maxLength, TdsDataType nullableType, TdsDataType... fixedTypes) {

        Assert.isTrue(nullableType.getLengthStrategy() != LengthStrategy.FIXEDLENTYPE, String.format("Type [%s] specified a fixed-length strategy in its nullable type", name));

        for (TdsDataType fixedType : fixedTypes) {
            Assert.isTrue(fixedType.getLengthStrategy() == LengthStrategy.FIXEDLENTYPE, String.format("Type [%s] specified [%s] in its fixed length type [%s] ", name,
                fixedType.getLengthStrategy(), fixedType));
        }

        this.category = category;
        this.defaultJavaType = defaultJavaType;
        this.name = name;
        this.maxLength = maxLength;
        this.nullableType = nullableType;
        this.fixedTypes = fixedTypes;
    }

    SqlServerType(Category category, Class<?> defaultJavaType, String name) {

        this.category = category;
        this.defaultJavaType = defaultJavaType;
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
    public static SqlServerType of(String typeName) {

        for (SqlServerType type : SqlServerType.values())
            if (type.name.equalsIgnoreCase(typeName)) {
                return type;
            }

        throw new IllegalArgumentException(String.format("Unknown type: %s", typeName));
    }

    /**
     * Resolve a {@link SqlServerType} by its {@link R2dbcType}.
     *
     * @param type the R2DBC type.
     * @return the resolved {@link SqlServerType}.
     * @throws IllegalArgumentException if the type name cannot be resolved to a {@link SqlServerType}
     */
    public static SqlServerType of(R2dbcType type) {

        switch (type) {

            case CHAR:
                return SqlServerType.CHAR;
            case VARCHAR:
                return SqlServerType.VARCHAR;
            case NCHAR:
                return SqlServerType.NCHAR;
            case NVARCHAR:
                return SqlServerType.NVARCHAR;
            case CLOB:
                return SqlServerType.TEXT;
            case NCLOB:
                return SqlServerType.NTEXT;
            case BOOLEAN:
                return SqlServerType.TINYINT;
            case BINARY:
                return SqlServerType.BINARY;
            case VARBINARY:
                return SqlServerType.VARBINARY;
            case BLOB:
                return SqlServerType.IMAGE;
            case INTEGER:
                return SqlServerType.INTEGER;
            case TINYINT:
                return SqlServerType.TINYINT;
            case SMALLINT:
                return SqlServerType.SMALLINT;
            case BIGINT:
                return SqlServerType.BIGINT;
            case NUMERIC:
                return SqlServerType.NUMERIC;
            case DECIMAL:
                return SqlServerType.DECIMAL;
            case FLOAT:
            case DOUBLE:
                return SqlServerType.FLOAT;
            case REAL:
                return SqlServerType.REAL;
            case DATE:
                return SqlServerType.DATE;
            case TIME:
                return SqlServerType.TIME;
            case TIMESTAMP:
                return SqlServerType.TIMESTAMP;
        }

        throw new IllegalArgumentException(String.format("Unsupported type: %s", type));
    }

    public Category getCategory() {
        return this.category;
    }

    @Override
    public Class<?> getJavaType() {
        return this.defaultJavaType;
    }

    public int getMaxLength() {
        return this.maxLength;
    }

    @Override
    public String getName() {
        return this.name;
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
    public enum Category {
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
