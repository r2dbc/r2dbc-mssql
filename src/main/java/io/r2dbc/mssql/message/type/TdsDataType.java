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

package io.r2dbc.mssql.message.type;

/**
 * SQL Server data types as represented within TDS.
 */
public enum TdsDataType {

    // @formatter:off
    // FIXEDLEN types
    BIT1(0x32), // 50
    INT8(0x7F), // 127
    INT4(0x38), // 56
    INT2(0x34), // 52
    INT1(0x30), // 48
    FLOAT4(0x3B), // 59
    FLOAT8(0x3E), // 62
    DATETIME4(0x3A), // 58
    DATETIME8(0x3D), // 61
    MONEY4(0x7A), // 122
    MONEY8(0x3C), // 60

    // BYTELEN types
    BITN(0x68), // 104
    INTN(0x26), // 38
    DECIMALN(0x6A), // 106
    NUMERICN(0x6C), // 108
    FLOATN(0x6D), // 109
    MONEYN(0x6E), // 110
    DATETIMEN(0x6F), // 111
    GUID(0x24), // 36
    DATEN(0x28), // 40
    TIMEN(0x29), // 41
    DATETIME2N(0x2a), // 42
    DATETIMEOFFSETN(0x2b), // 43

    // USHORTLEN type
    BIGCHAR(0xAF), // -81
    BIGVARCHAR(0xA7), // -89
    BIGBINARY(0xAD), // -83
    BIGVARBINARY(0xA5), // -91
    NCHAR(0xEF), // -17
    NVARCHAR(0xE7), // -15

    // PARTLEN types
    IMAGE(0x22), // 34
    TEXT(0x23), // 35
    NTEXT(0x63), // 99
    UDT(0xF0), // -16
    XML(0xF1), // -15

    // LONGLEN types
    SQL_VARIANT(0x62); // 98

    // @formatter:on

    private static final int MAXELEMENTS = 256;

    private static final TdsDataType cache[] = new TdsDataType[MAXELEMENTS];

    private final int value;

    static {
        for (TdsDataType s : values())
            cache[s.value] = s;
    }

    TdsDataType(int value) {
        this.value = value;
    }

    public byte getValue() {
        return (byte) value;
    }

    /**
     * Return the {@link TdsDataType} by looking it up using the given type value.
     *
     * @param value the data type.
     * @return the {@link TdsDataType}.
     */
    static TdsDataType valueOf(int value) {

        if (value > MAXELEMENTS || value < 0 || cache[value] == null) {
            throw new IllegalArgumentException(String.format("Invalid TDS type: %d", value));
        }

        return cache[value];
    }
}
