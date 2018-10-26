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
import io.r2dbc.mssql.codec.LengthDescriptor;
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
     * @param columns column descriptors.
     * @param data    the row data.
     */
    private RowToken(List<ByteBuf> data, ReferenceCounted toRelease) {

        this.data = Objects.requireNonNull(data, "Row data must not be null");
        this.toRelease = toRelease;
    }

    /**
     * Decode a {@link RowToken}.
     *
     * @param buffer
     * @param columns @param columns column descriptors.
     * @param nbc     {@literal null} bit compressed map to indicate {@literal null} values for column values.
     * @return the {@link RowToken}.
     */
    public static RowToken decode(ByteBuf buffer, List<Column> columns) {

        ByteBuf copy = buffer.copy();

        int start = copy.readerIndex();
        RowToken rowToken = doDecode(columns, copy);
        int fastForward = copy.readerIndex() - start;

        buffer.skipBytes(fastForward);

        return rowToken;
    }

    private static RowToken doDecode(List<Column> columns, ByteBuf buffer) {

        List<ByteBuf> data = new ArrayList<>(columns.size());

        for (Column column : columns) {

            buffer.markReaderIndex();
            int startRead = buffer.readerIndex();
            LengthDescriptor lengthDescriptor = LengthDescriptor.decode(buffer, column);
            int endRead = buffer.readerIndex();
            buffer.resetReaderIndex();

            int descriptorLength = endRead - startRead;
            ByteBuf columnData = buffer.readSlice(descriptorLength + lengthDescriptor.getLength());
            data.add(columnData);
        }

        return new RowToken(data, buffer);
    }

    /**
     * Returns the {@link ByteBuf data} for the column at {@code index}.
     *
     * @param index the column {@code index}.
     * @return the data buffer. Can be {@literal null} if indicated by null-bit compression.
     */
    @Nullable
    public ByteBuf getColumnData(int index) {
        return data.get(index);
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String getName() {
        return "ROW_TOKEN";
    }

    @Override
    public RowToken touch(Object hint) {

        toRelease.touch(hint);
        return this;
    }

    @Override
    protected void deallocate() {
        toRelease.release();
    }
}
