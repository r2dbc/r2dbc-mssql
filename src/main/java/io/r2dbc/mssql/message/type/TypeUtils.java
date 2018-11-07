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

import io.r2dbc.mssql.util.Assert;

/**
 * Helper methods for Type-specific calculations.
 *
 * @author Mark Paluch
 */
public final class TypeUtils {

    /* System defined UDTs */
    static final int UDT_TIMESTAMP = 0x0050;

    /**
     * Max length in Unicode characters allowed by the "short" NVARCHAR type. Values longer than this must use
     * NVARCHAR(max) (Yukon or later) or NTEXT (Shiloh)
     */
    static final int SHORT_VARTYPE_MAX_CHARS = 4000;

    /**
     * Max length in bytes allowed by the "short" VARBINARY/VARCHAR types. Values longer than this must use
     * VARBINARY(max)/VARCHAR(max) (Yukon or later) or IMAGE/TEXT (Shiloh)
     */
    public static final int SHORT_VARTYPE_MAX_BYTES = 8000;

    /**
     * A type with unlimited max size, known as varchar(max), varbinary(max) and nvarchar(max), which has a max size of
     * 0xFFFF, defined by PARTLENTYPE.
     */
    static final int SQL_USHORTVARMAXLEN = 65535; // 0xFFFF

    /**
     * From SQL Server 2005 Books Online : ntext, text, and image (Transact-SQL)
     * http://msdn.microsoft.com/en-us/library/ms187993.aspx
     * <p>
     * image "... through 2^31 - 1 (2,147,483,687) bytes."
     * <p>
     * text "... maximum length of 2^31 - 1 (2,147,483,687) characters."
     * <p>
     * ntext "... maximum length of 2^30 - 1 (1,073,741,823) characters."
     */
    static final int NTEXT_MAX_CHARS = 0x3FFFFFFF;

    public static final int IMAGE_TEXT_MAX_BYTES = 0x7FFFFFFF;

    /**
     * Transact-SQL Data Types: http://msdn.microsoft.com/en-us/library/ms179910.aspx
     * <p/>
     * {@literal varbinary(max)} "max indicates that the maximum storage size is 2<sup>31</sup> - 1 bytes. The storage size is the actual
     * length of the data entered + 2 bytes."
     * <p/>
     * {@literal varchar(max)} "max indicates that the maximum storage size is 2<sup>31</sup> - 1 bytes. The storage size is the actual
     * length of the data entered + 2 bytes."
     * <p/>
     * {@literal nvarchar(max)} "max indicates that the maximum storage size is 2<sup>31</sup> - 1 bytes. The storage size, in bytes, is two
     * times the number of characters entered + 2 bytes."
     * <p/>
     * Normally, that would mean that the maximum length of {@literal nvarchar(max)} data is 0x3FFFFFFE characters and that the
     * maximum length of {@literal varchar(max)} or {@literal varbinary(max)} data is 0x3FFFFFFD bytes. Despite the documentation,
     * SQL Server returns 2<sup>30</sup> - 1 and 2<sup>31</sup> - 1 respectively as the PRECISION of these types, so use that instead.
     */
    static final int MAX_VARTYPE_MAX_CHARS = 0x3FFFFFFF;

    static final int MAX_VARTYPE_MAX_BYTES = 0x7FFFFFFF;

    // Special length indicator for varchar(max), nvarchar(max) and varbinary(max).
    static final int MAXTYPE_LENGTH = 0xFFFF;

    public static final int UNKNOWN_STREAM_LENGTH = -1;

    public static final int MAX_FRACTIONAL_SECONDS_SCALE = 7;

    public static final int DAYS_INTO_CE_LENGTH = 3;

    public static final int MINUTES_OFFSET_LENGTH = 2;

    // Number of days in a "normal" (non-leap) year according to SQL Server.
    static final int DAYS_PER_YEAR = 365;

    private static final int[] SCALED_TIME_LENGTHS = new int[]{3, 3, 3, 4, 4, 5, 5, 5};

    /**
     * Returns the length of time values using {@code scale}.
     *
     * @param scale the time scale.
     * @return length of a time value.
     */
    public static int getTimeValueLength(int scale) {
        return getNanosSinceMidnightLength(scale);
    }

    /**
     * Returns the length of Date-Time2 values using {@code scale}.
     *
     * @param scale the time scale.
     * @return length of a time value.
     */
    public static int getDateTimeValueLength(int scale) {
        return DAYS_INTO_CE_LENGTH + getNanosSinceMidnightLength(scale);
    }

    /**
     * Returns the length of Date-Time offset values using {@code scale}.
     *
     * @param scale the scale.
     * @return length of Date-Time offset values.
     */
    static int getDatetimeoffsetValueLength(int scale) {
        return DAYS_INTO_CE_LENGTH + MINUTES_OFFSET_LENGTH + getNanosSinceMidnightLength(scale);
    }

    /**
     * Returns the length of Time offset values using {@code scale}.
     *
     * @param scale the scale.
     * @return
     */
    private static int getNanosSinceMidnightLength(int scale) {

        Assert.isTrue(scale >= 0 && scale <= MAX_FRACTIONAL_SECONDS_SCALE, "Scale must be between 0 and 7");

        return SCALED_TIME_LENGTHS[scale];
    }
}
