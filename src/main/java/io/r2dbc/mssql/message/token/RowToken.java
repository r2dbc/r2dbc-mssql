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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.message.type.Length;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Row token message containing row bytes.
 *
 * @author Mark Paluch
 */
public class RowToken extends AbstractReferenceCounted implements DataToken {

    public static final byte TYPE = (byte) 0xD1;

    private final List<ByteBuf> data;

    private final ReferenceCounted toRelease;

    /**
     * Creates a {@link RowToken}.
     *
     * @param data      the row data.
     * @param toRelease item to {@link ReferenceCounted#release()} on {@link #deallocate() de-allocation}.
     */
    RowToken(List<ByteBuf> data, ReferenceCounted toRelease) {

        this.data = Objects.requireNonNull(data, "Row data must not be null");
        this.toRelease = toRelease;
    }

    /**
     * Decode a {@link RowToken}.
     *
     * @param buffer  the data buffer.
     * @param columns column descriptors.
     * @return the {@link RowToken}.
     */
    public static RowToken decode(ByteBuf buffer, List<Column> columns) {

        Objects.requireNonNull(buffer, "Data buffer must not be null");
        Objects.requireNonNull(columns, "List of Columns must not be null");

        ByteBuf copy = buffer.copy();

        int start = copy.readerIndex();
        RowToken rowToken = doDecode(copy, columns);
        int fastForward = copy.readerIndex() - start;

        buffer.skipBytes(fastForward);

        return rowToken;
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link RowToken}.
     *
     * @param buffer  the data buffer.
     * @param columns column descriptors.
     * @return {@literal true} if the buffer contains sufficient data to entirely decode a row.
     */
    public static boolean canDecode(ByteBuf buffer, List<Column> columns) {

        Objects.requireNonNull(buffer, "Data buffer must not be null");
        Objects.requireNonNull(columns, "List of Columns must not be null");

        int readerIndex = buffer.readerIndex();

        try {

            for (Column column : columns) {

                if (!canDecodeColumn(buffer, column)) {
                    return false;
                }
            }

            return true;
        } finally {
            buffer.readerIndex(readerIndex);
        }
    }

    static boolean canDecodeColumn(ByteBuf buffer, Column column) {

        buffer.markReaderIndex();

        int startRead = buffer.readerIndex();

        if (!Length.canDecode(buffer, column.getType())) {
            return false;
        }

        Length length = Length.decode(buffer, column.getType());

        int endRead = buffer.readerIndex();
        buffer.resetReaderIndex();

        int descriptorLength = endRead - startRead;
        int dataLength = descriptorLength + length.getLength();

        if (buffer.readableBytes() >= dataLength) {
            buffer.skipBytes(dataLength);
            return true;
        }

        return false;
    }

    private static RowToken doDecode(ByteBuf buffer, List<Column> columns) {

        List<ByteBuf> data = new ArrayList<>(columns.size());

        for (Column column : columns) {
            data.add(decodeColumnData(buffer, column));
        }

        return new RowToken(data, buffer);
    }

    /**
     * Decode a {@link ByteBuf data buffer} for a single {@link Column}.
     *
     * @param buffer the data buffer.
     * @param column the column.
     * @return
     */
    static ByteBuf decodeColumnData(ByteBuf buffer, Column column) {

        buffer.markReaderIndex();
        int startRead = buffer.readerIndex();
        Length length = Length.decode(buffer, column.getType());
        int endRead = buffer.readerIndex();
        buffer.resetReaderIndex();

        int descriptorLength = endRead - startRead;
        return buffer.readSlice(descriptorLength + length.getLength());
    }

    /**
     * Returns the {@link ByteBuf data} for the column at {@code index}.
     *
     * @param index the column {@code index}.
     * @return the data buffer. Can be {@literal null} if indicated by null-bit compression.
     */
    @Nullable
    public ByteBuf getColumnData(int index) {
        return this.data.get(index);
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String getName() {
        return "ROW";
    }

    @Override
    public RowToken touch(Object hint) {

        this.toRelease.touch(hint);
        return this;
    }

    @Override
    protected void deallocate() {
        this.toRelease.release();
    }
}
