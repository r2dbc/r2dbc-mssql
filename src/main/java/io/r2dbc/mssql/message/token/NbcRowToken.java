/*
 * Copyright 2018-2022 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.util.Assert;

import java.util.Arrays;

/**
 * NBC Row (Null-bitmap compressed row). Expresses nullability through a bitmap.
 * <p><strong>Note:</strong> PLP values are aggregated in a single {@link ByteBuf} and not yet streamed. This is to be fixed.
 *
 * @author Mark Paluch
 */
public final class NbcRowToken extends RowToken {

    public static final byte TYPE = (byte) 0xD2;

    private final boolean[] nullMarker;

    /**
     * Creates a {@link NbcRowToken}.
     *
     * @param data       the row data.
     * @param nullMarker {@code null} bitmap.
     */
    private NbcRowToken(ByteBuf[] data, boolean[] nullMarker) {
        super(data);
        this.nullMarker = nullMarker;
    }

    /**
     * Decode a {@link NbcRowToken}.
     *
     * @param buffer  the data buffer.
     * @param columns column descriptors.
     * @return the {@link RowToken}.
     */
    public static NbcRowToken decode(ByteBuf buffer, Column[] columns) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");
        Assert.requireNonNull(columns, "List of Columns must not be null");

        return doDecode(buffer, columns);
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link NbcRowToken}.
     *
     * @param buffer  the data buffer.
     * @param columns column descriptors.
     * @return {@code true} if the buffer contains sufficient data to entirely decode a row.
     */
    public static boolean canDecode(ByteBuf buffer, Column[] columns) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");
        Assert.requireNonNull(columns, "List of Columns must not be null");

        int readerIndex = buffer.readerIndex();
        int nullBitmapSize = getNullBitmapSize(columns);
        try {

            if (buffer.readableBytes() < nullBitmapSize) {
                return false;
            }

            boolean[] nullBitmap = getNullBitmap(buffer, columns);

            for (int i = 0; i < columns.length; i++) {

                Column column = columns[i];

                if (nullBitmap[i]) {
                    continue;
                }

                if (!canDecodeColumn(buffer, column)) {
                    return false;
                }
            }

            return true;
        } finally {
            buffer.readerIndex(readerIndex);
        }
    }

    @Override
    public ByteBuf getColumnData(int index) {
        return this.nullMarker[index] ? null : super.getColumnData(index);
    }

    private static NbcRowToken doDecode(ByteBuf buffer, Column[] columns) {

        ByteBuf[] data = new ByteBuf[columns.length];

        boolean[] nullMarkers = getNullBitmap(buffer, columns);

        for (int i = 0; i < columns.length; i++) {

            Column column = columns[i];

            if (nullMarkers[i]) {
                data[i] = Unpooled.EMPTY_BUFFER;
            } else {
                data[i] = decodeColumnData(buffer, column);
            }
        }

        return new NbcRowToken(data, nullMarkers);
    }

    private static boolean[] getNullBitmap(ByteBuf buffer, Column[] columns) {

        int nullBitmapSize = getNullBitmapSize(columns);

        boolean[] nullMarkers = new boolean[columns.length];
        int column = 0;

        for (int byteNo = 0; byteNo < nullBitmapSize; byteNo++) {

            byte byteValue = Decode.asByte(buffer);

            // if this byte is 0, skip to the next byte
            // and increment the column number by 8(no of bits)
            if (byteValue == 0) {
                column = column + 8;
                continue;
            }

            for (int bitNo = 0; bitNo < 8 && column < columns.length; bitNo++) {
                if ((byteValue & (1 << bitNo)) != 0) {
                    nullMarkers[column] = true;
                }
                column++;
            }
        }
        return nullMarkers;
    }

    private static int getNullBitmapSize(Column[] columns) {
        return ((columns.length - 1) >> 3) + 1;
    }

    @Override
    public String getName() {
        return "NBCROW";
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getName());
        sb.append(" [nullMarker=").append(Arrays.toString(this.nullMarker));
        sb.append(']');
        return sb.toString();
    }

}
