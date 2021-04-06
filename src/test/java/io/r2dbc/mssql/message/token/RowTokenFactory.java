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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.util.TestByteBufAllocator;

import java.util.List;
import java.util.function.Consumer;

/**
 * Utility to create a {@link RowToken}.
 *
 * @author Mark Paluch
 */
public final class RowTokenFactory {

    /**
     * Creates a {@link RowToken} using {@link ColumnMetadataToken} and an encoded data buffer.
     *
     * @param columnMetadata
     * @param bufferWriter
     * @return
     */
    public static RowToken create(ColumnMetadataToken columnMetadata, Consumer<ByteBuf> bufferWriter) {
        return create(columnMetadata.getColumns(), bufferWriter);
    }

    /**
     * Creates a {@link RowToken} using {@link Column}s and an encoded data buffer.
     *
     * @param columns
     * @param bufferWriter
     * @return
     */
    public static RowToken create(List<Column> columns, Consumer<ByteBuf> bufferWriter) {

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();

        bufferWriter.accept(buffer);

        return RowToken.decode(buffer, columns.toArray(new Column[0]));
    }

    /**
     * Creates a {@link RowToken} using {@link Column}s and an encoded data buffer.
     *
     * @param columns
     * @param bufferWriter
     * @return
     */
    public static RowToken create(Column[] columns, Consumer<ByteBuf> bufferWriter) {

        ByteBuf buffer = TestByteBufAllocator.TEST.buffer();

        bufferWriter.accept(buffer);

        return RowToken.decode(buffer, columns);
    }

    private RowTokenFactory() {
    }
}
