/*
 * Copyright 2018-2019 the original author or authors.
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
    BIT1(0x32, LengthStrategy.FIXEDLENTYPE), // 50
    INT8(0x7F, LengthStrategy.FIXEDLENTYPE), // 127
    INT4(0x38, LengthStrategy.FIXEDLENTYPE), // 56
    INT2(0x34, LengthStrategy.FIXEDLENTYPE), // 52
    INT1(0x30, LengthStrategy.FIXEDLENTYPE), // 48
    FLOAT4(0x3B, LengthStrategy.FIXEDLENTYPE), // 59
    FLOAT8(0x3E, LengthStrategy.FIXEDLENTYPE), // 62
    DATETIME4(0x3A, LengthStrategy.FIXEDLENTYPE), // 58
    DATETIME8(0x3D, LengthStrategy.FIXEDLENTYPE), // 61
    MONEY4(0x7A, LengthStrategy.FIXEDLENTYPE), // 122
    MONEY8(0x3C, LengthStrategy.FIXEDLENTYPE), // 60

    // BYTELEN types
    BITN(0x68, LengthStrategy.BYTELENTYPE), // 104
    INTN(0x26, LengthStrategy.BYTELENTYPE), // 38
    DECIMALN(0x6A, LengthStrategy.BYTELENTYPE), // 106
    NUMERICN(0x6C, LengthStrategy.BYTELENTYPE), // 108
    FLOATN(0x6D, LengthStrategy.BYTELENTYPE), // 109
    MONEYN(0x6E, LengthStrategy.BYTELENTYPE), // 110
    DATETIMEN(0x6F, LengthStrategy.BYTELENTYPE), // 111
    GUID(0x24, LengthStrategy.BYTELENTYPE), // 36
    DATEN(0x28, LengthStrategy.BYTELENTYPE), // 40
    TIMEN(0x29, LengthStrategy.BYTELENTYPE), // 41
    DATETIME2N(0x2a, LengthStrategy.BYTELENTYPE), // 42
    DATETIMEOFFSETN(0x2b, LengthStrategy.BYTELENTYPE), // 43

    // USHORTLEN type
    BIGCHAR(0xAF, LengthStrategy.USHORTLENTYPE), // -81
    BIGVARCHAR(0xA7, LengthStrategy.USHORTLENTYPE), // -89
    BIGBINARY(0xAD, LengthStrategy.USHORTLENTYPE), // -83
    BIGVARBINARY(0xA5, LengthStrategy.USHORTLENTYPE), // -91
    NCHAR(0xEF, LengthStrategy.USHORTLENTYPE), // -17
    NVARCHAR(0xE7, LengthStrategy.USHORTLENTYPE), // -15

    // PARTLEN types
    IMAGE(0x22, LengthStrategy.PARTLENTYPE), // 34
    TEXT(0x23, LengthStrategy.PARTLENTYPE), // 35
    NTEXT(0x63, LengthStrategy.PARTLENTYPE), // 99
    UDT(0xF0, LengthStrategy.PARTLENTYPE), // -16
    XML(0xF1, LengthStrategy.PARTLENTYPE), // -15

    // LONGLEN types
    SQL_VARIANT(0x62, LengthStrategy.LONGLENTYPE); // 98

    // @formatter:on

    private static final int MAXELEMENTS = 256;

    private static final TdsDataType cache[] = new TdsDataType[MAXELEMENTS];

    private final int value;

    private final LengthStrategy lengthStrategy;

    static {
        for (TdsDataType s : values())
            cache[s.value] = s;
    }

    TdsDataType(int value, LengthStrategy lengthStrategy) {
        this.value = value;
        this.lengthStrategy = lengthStrategy;
    }

    public byte getValue() {
        return (byte) this.value;
    }

    public LengthStrategy getLengthStrategy() {
        return this.lengthStrategy;
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
