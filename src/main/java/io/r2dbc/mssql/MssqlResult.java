/*
 * Copyright 2018-2019 the original author or authors.
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

package io.r2dbc.mssql;

import io.netty.util.ReferenceCountUtil;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.token.AbstractDoneToken;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.NbcRowToken;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

/**
 * {@link Result} of query results.
 *
 * @author Mark Paluch
 */
public final class MssqlResult implements Result {

    private static final Logger logger = LoggerFactory.getLogger(MssqlResult.class);

    private final Codecs codecs;

    private final Flux<Message> messages;

    private volatile MssqlRowMetadata rowMetadata;

    private volatile Throwable throwable;

    public MssqlResult(Codecs codecs, Flux<Message> messages) {

        this.codecs = codecs;
        this.messages = messages;
    }

    /**
     * Create a {@link MssqlResult}.
     *
     * @param codecs   the codecs to use.
     * @param messages message stream.
     * @return {@link Result} object.
     */
    static MssqlResult toResult(Codecs codecs, Flux<Message> messages) {

        Assert.requireNonNull(codecs, "Codecs must not be null");
        Assert.requireNonNull(messages, "Messages must not be null");

        logger.debug("Creating new result");

        return new MssqlResult(codecs, messages);
    }

    @Override
    public Mono<Integer> getRowsUpdated() {

        return messages()
            .<Long>handle((message, sink) -> {

                ReferenceCountUtil.release(message);

                if (message instanceof AbstractDoneToken) {

                    AbstractDoneToken doneToken = (AbstractDoneToken) message;
                    if (doneToken.hasCount()) {

                        if (logger.isDebugEnabled()) {
                            logger.debug("Incoming row count: {}", doneToken);
                        }

                        sink.next(doneToken.getRowCount());
                    }
                }
            }).reduce(Long::sum).map(Long::intValue);
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {

        Assert.requireNonNull(f, "Mapping function must not be null");


        Flux<MssqlRow> rows = messages()
            .handle((message, sink) -> {

                if (message.getClass() == ColumnMetadataToken.class) {

                    ColumnMetadataToken token = (ColumnMetadataToken) message;

                    if (token.getColumns().isEmpty()) {
                        return;
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("Result column definition: {}", message);
                    }

                    this.rowMetadata = MssqlRowMetadata.create(codecs, token);

                    return;
                }

                if (message.getClass() == RowToken.class || message.getClass() == NbcRowToken.class) {

                    MssqlRowMetadata rowMetadata = this.rowMetadata;

                    if (rowMetadata == null) {
                        sink.error(new IllegalStateException("No MssqlRowMetadata available"));
                        return;
                    }
                    sink.next(MssqlRow.toRow(codecs, (RowToken) message, rowMetadata));
                    return;
                }

                ReferenceCountUtil.release(message);
            });

        return rows
            .map((row) -> {
                try {
                    return f.apply(row, row.getMetadata());
                } finally {
                    row.release();
                }
            });
    }

    private Flux<Message> messages() {

        return messages.handle((message, sink) -> {

            Throwable exception = this.throwable;

            if (AbstractDoneToken.isDone(message)) {
                if (exception != null) {
                    sink.error(exception);
                    return;
                }
            }

            if (message instanceof ErrorToken) {

                MssqlException mssqlException = MssqlException.create((ErrorToken) message);

                if (exception != null) {
                    exception.addSuppressed(mssqlException);
                } else {
                    this.throwable = mssqlException;
                }

                return;
            }

            sink.next(message);
        });
    }
}
