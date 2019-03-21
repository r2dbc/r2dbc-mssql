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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * NBC Row (Null-bitmap compressed row). Expresses nullability through a bitmap.
 *
 * @author Mark Paluch
 */
public class NbcRowToken extends RowToken {

    public static final byte TYPE = (byte) 0xD2;

    private final boolean[] nullMarker;

    /**
     * Creates a {@link NbcRowToken}.
     *
     * @param data       the row data.
     * @param toRelease  item to {@link ReferenceCounted#release()} on {@link #deallocate() de-allocation}.
     * @param nullMarker {@code null} bitmap.
     */
    private NbcRowToken(List<ByteBuf> data, ReferenceCounted toRelease, boolean[] nullMarker) {
        super(data, toRelease);
        this.nullMarker = nullMarker;
    }

    /**
     * Decode a {@link NbcRowToken}.
     *
     * @param buffer  the data buffer.
     * @param columns column descriptors.
     * @return the {@link RowToken}.
     */
    public static NbcRowToken decode(ByteBuf buffer, List<Column> columns) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");
        Assert.requireNonNull(columns, "List of Columns must not be null");

        ByteBuf copy = buffer.copy();

        int start = copy.readerIndex();
        NbcRowToken rowToken = doDecode(copy, columns);
        int fastForward = copy.readerIndex() - start;

        buffer.skipBytes(fastForward);

        return rowToken;
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link NbcRowToken}.
     *
     * @param buffer  the data buffer.
     * @param columns column descriptors.
     * @return {@literal true} if the buffer contains sufficient data to entirely decode a row.
     */
    public static boolean canDecode(ByteBuf buffer, List<Column> columns) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");
        Assert.requireNonNull(columns, "List of Columns must not be null");

        int readerIndex = buffer.readerIndex();
        int nullBitmapSize = getNullBitmapSize(columns);
        try {

            if (buffer.readableBytes() < nullBitmapSize) {
                return false;
            }

            boolean[] nullBitmap = getNullBitmap(buffer, columns);

            for (int i = 0; i < columns.size(); i++) {

                Column column = columns.get(i);

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

    private static NbcRowToken doDecode(ByteBuf buffer, List<Column> columns) {

        List<ByteBuf> data = new ArrayList<>(columns.size());

        boolean[] nullMarkers = getNullBitmap(buffer, columns);

        for (int i = 0; i < columns.size(); i++) {

            Column column = columns.get(i);

            if (nullMarkers[i]) {
                data.add(Unpooled.EMPTY_BUFFER);
            } else {
                data.add(decodeColumnData(buffer, column));
            }
        }

        return new NbcRowToken(data, buffer, nullMarkers);
    }

    private static boolean[] getNullBitmap(ByteBuf buffer, List<Column> columns) {

        int nullBitmapSize = getNullBitmapSize(columns);

        boolean[] nullMarkers = new boolean[columns.size()];
        int column = 0;

        for (int byteNo = 0; byteNo < nullBitmapSize; byteNo++) {

            byte byteValue = Decode.asByte(buffer);

            // if this byte is 0, skip to the next byte
            // and increment the column number by 8(no of bits)
            if (byteValue == 0) {
                column = column + 8;
                continue;
            }

            for (int bitNo = 0; bitNo < 8 && column < columns.size(); bitNo++) {
                if ((byteValue & (1 << bitNo)) != 0) {
                    nullMarkers[column] = true;
                }
                column++;
            }
        }
        return nullMarkers;
    }

    private static int getNullBitmapSize(List<Column> columns) {
        return ((columns.size() - 1) >> 3) + 1;
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
