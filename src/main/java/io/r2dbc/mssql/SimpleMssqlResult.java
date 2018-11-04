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

package io.r2dbc.mssql;

import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.token.AbstractDoneToken;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.BiFunction;

import static reactor.function.TupleUtils.function;

/**
 * Simple, non-cursored {@link Result} of query results.
 *
 * @author Mark Paluch
 */
final class SimpleMssqlResult implements Result {

    private final Flux<MssqlRow> rows;

    private final Mono<Long> rowsUpdated;

    /**
     * Creates a new {@link SimpleMssqlResult}.
     *
     * @param rows        stream of {@link MssqlRow}.
     * @param rowsUpdated publisher of the updated row count.
     */
    SimpleMssqlResult(Flux<MssqlRow> rows, Mono<Long> rowsUpdated) {
        this.rows = rows;
        this.rowsUpdated = rowsUpdated;
    }

    @Override
    public Publisher<Integer> getRowsUpdated() {
        return this.rowsUpdated.map(Long::intValue);
    }

    @Override
    public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {

        Objects.requireNonNull(f, "Mapping function must not be null");

        return this.rows
            .map((row) -> {
                try {
                    return f.apply(row, new MssqlRowMetadata(row));
                } finally {
                    row.release();
                }
            });
    }

    /**
     * Create a non-cursored {@link SimpleMssqlResult}.
     *
     * @param codecs   the codecs to use.
     * @param messages message stream.
     * @return {@link Result} object.
     */
    static SimpleMssqlResult toResult(Codecs codecs, Flux<Message> messages) {

        Objects.requireNonNull(codecs, "Codecs must not be null");
        Objects.requireNonNull(messages, "Messages must not be null");

        EmitterProcessor<Message> processor = EmitterProcessor.create(false);

        Flux<Message> firstMessages = processor.take(3).cache();

        Mono<ColumnMetadataToken> columnDescriptions = firstMessages
            .ofType(ColumnMetadataToken.class)
            .singleOrEmpty()
            .cache();

        Flux<MssqlRow> rows = processor
            .startWith(firstMessages)
            .ofType(RowToken.class)
            .zipWith(columnDescriptions.repeat())
            .map(function((dataToken, columns) -> MssqlRow.toRow(codecs, dataToken, columns)));

        Mono<Long> rowsUpdated = firstMessages
            .doOnNext(it -> {

                // Release unused tokens directly.
                if (it instanceof ReferenceCounted) {
                    ((ReferenceCounted) it).release();
                }
            })
            .ofType(AbstractDoneToken.class)
            .filter(AbstractDoneToken::hasCount)
            .map(AbstractDoneToken::getRowCount)
            .singleOrEmpty();

        messages
            .handle(MssqlException::handleErrorResponse)
            .hide()
            .subscribe(processor);

        return new SimpleMssqlResult(rows, rowsUpdated);
    }
}
