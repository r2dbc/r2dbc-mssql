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

package io.r2dbc.mssql;

import io.netty.util.ReferenceCountUtil;
import io.r2dbc.mssql.client.ConnectionContext;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.message.token.AbstractDoneToken;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.NbcRowToken;
import io.r2dbc.mssql.message.token.ReturnValue;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * {@link Result} of query results.
 *
 * @author Mark Paluch
 */
final class DefaultMssqlResult implements MssqlResult {

    private static final Logger LOGGER = Loggers.getLogger(DefaultMssqlResult.class);

    public static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();

    private final String sql;

    private final ConnectionContext context;

    private final Codecs codecs;

    private final Flux<io.r2dbc.mssql.message.Message> messages;

    private final boolean expectReturnValues;

    private volatile MssqlRowMetadata rowMetadata;

    private volatile RuntimeException throwable;

    private DefaultMssqlResult(String sql, ConnectionContext context, Codecs codecs, Flux<io.r2dbc.mssql.message.Message> messages, boolean expectReturnValues) {

        this.sql = sql;
        this.context = context;
        this.codecs = codecs;
        this.messages = messages;
        this.expectReturnValues = expectReturnValues;
    }

    /**
     * Create a {@link DefaultMssqlResult}.
     *
     * @param sql                the underlying SQL statement.
     * @param codecs             the codecs to use.
     * @param messages           message stream.
     * @param expectReturnValues {@code true} if the result is expected to have result values.
     * @return {@link Result} object.
     */
    static MssqlResult toResult(String sql, ConnectionContext context, Codecs codecs, Flux<io.r2dbc.mssql.message.Message> messages, boolean expectReturnValues) {

        Assert.requireNonNull(sql, "SQL must not be null");
        Assert.requireNonNull(codecs, "Codecs must not be null");
        Assert.requireNonNull(context, "ConnectionContext must not be null");
        Assert.requireNonNull(messages, "Messages must not be null");

        LOGGER.debug(context.getMessage("Creating new result"));

        return new DefaultMssqlResult(sql, context, codecs, messages, expectReturnValues);
    }

    @Override
    public Mono<Integer> getRowsUpdated() {

        return this.messages
            .<Long>handle((message, sink) -> {

                if (message instanceof AbstractDoneToken) {

                    AbstractDoneToken doneToken = (AbstractDoneToken) message;
                    if (doneToken.hasCount()) {

                        if (DEBUG_ENABLED) {
                            LOGGER.debug(this.context.getMessage("Incoming row count: {}"), doneToken);
                        }

                        sink.next(doneToken.getRowCount());
                    }

                    if (doneToken.isAttentionAck()) {
                        sink.error(new ExceptionFactory.MssqlStatementCancelled());
                        return;
                    }
                }

                if (message instanceof ErrorToken) {

                    R2dbcException mssqlException = ExceptionFactory.createException((ErrorToken) message, this.sql);

                    Throwable exception = this.throwable;
                    if (exception != null) {
                        exception.addSuppressed(mssqlException);
                    } else {
                        this.throwable = mssqlException;
                    }

                    return;
                }

                ReferenceCountUtil.release(message);
            }).doOnComplete(() -> {
                RuntimeException exception = this.throwable;
                if (exception != null) {
                    throw exception;
                }
            }).reduce(Long::sum).map(Long::intValue);
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
        Assert.requireNonNull(mappingFunction, "Mapping function must not be null");
        return doMap(true, false, readable -> {
            Row row = (Row) readable;
            return mappingFunction.apply(row, row.getMetadata());
        });
    }

    @Override
    public <T> Publisher<T> map(Function<? super Readable, ? extends T> mappingFunction) {
        Assert.requireNonNull(mappingFunction, "Mapping function must not be null");
        return doMap(true, true, mappingFunction);
    }

    private <T> Flux<T> doMap(boolean rows, boolean outparameters, Function<? super Readable, ? extends T> mappingFunction) {

        Flux<T> mappedReturnValues = Flux.empty();
        Flux<io.r2dbc.mssql.message.Message> messages = this.messages;

        if (this.expectReturnValues && outparameters) {

            List<ReturnValue> returnValues = new ArrayList<>();

            messages = messages.doOnNext(message -> {

                if (message instanceof ReturnValue) {
                    returnValues.add((ReturnValue) message);
                }
            }).filter(it -> !(it instanceof ReturnValue));

            mappedReturnValues = Flux.defer(() -> {

                if (returnValues.size() != 0) {

                    MssqlReturnValues mssqlReturnValues = MssqlReturnValues.toReturnValues(this.codecs, returnValues);

                    try {
                        return Flux.just(mappingFunction.apply(mssqlReturnValues));
                    } finally {
                        mssqlReturnValues.release();
                    }
                }

                return Flux.empty();
            });
        }

        Flux<T> mapped = messages
            .handle((message, sink) -> {

                if (message instanceof AbstractDoneToken) {

                    AbstractDoneToken doneToken = (AbstractDoneToken) message;
                    if (doneToken.isAttentionAck()) {
                        sink.error(new ExceptionFactory.MssqlStatementCancelled());
                        return;
                    }
                }

                if (message.getClass() == ColumnMetadataToken.class) {

                    ColumnMetadataToken token = (ColumnMetadataToken) message;

                    if (!token.hasColumns()) {
                        return;
                    }

                    if (DEBUG_ENABLED) {
                        LOGGER.debug(this.context.getMessage("Result column definition: {}"), message);
                    }

                    this.rowMetadata = MssqlRowMetadata.create(this.codecs, token);

                    return;
                }

                if (rows && (message.getClass() == RowToken.class || message.getClass() == NbcRowToken.class)) {

                    MssqlRowMetadata rowMetadata = this.rowMetadata;

                    if (rowMetadata == null) {
                        sink.error(new IllegalStateException("No MssqlRowMetadata available"));
                        return;
                    }

                    MssqlRow row = MssqlRow.toRow(this.codecs, (RowToken) message, rowMetadata);
                    try {
                        sink.next(mappingFunction.apply(row));
                    } finally {
                        row.release();
                    }

                    return;
                }

                if (message instanceof ErrorToken) {

                    R2dbcException mssqlException = ExceptionFactory.createException((ErrorToken) message, this.sql);

                    Throwable exception = this.throwable;
                    if (exception != null) {
                        exception.addSuppressed(mssqlException);
                    } else {
                        this.throwable = mssqlException;
                    }

                    return;
                }

                if (this.expectReturnValues && message instanceof ReturnValue) {
                    return;
                }

                ReferenceCountUtil.release(message);
            });

        if (this.expectReturnValues) {
            mapped = mapped.concatWith(mappedReturnValues);
        }

        return mapped.doOnComplete(() -> {
            RuntimeException exception = this.throwable;
            if (exception != null) {
                throw exception;
            }
        });
    }

    @Override
    public MssqlResult filter(Predicate<Segment> filter) {
        return MssqlSegmentResult.toResult(this.sql, this.context, this.codecs, this.messages, this.expectReturnValues).filter(filter);
    }

    @Override
    public <T> Flux<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
        return MssqlSegmentResult.toResult(this.sql, this.context, this.codecs, this.messages, this.expectReturnValues).flatMap(mappingFunction);
    }

}
