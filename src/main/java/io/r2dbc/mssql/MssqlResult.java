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
import io.r2dbc.mssql.message.token.AbstractInfoToken;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.NbcRowToken;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * {@link Result} of query results.
 *
 * @author Mark Paluch
 */
public final class MssqlResult implements Result {

    private static final Logger LOGGER = Loggers.getLogger(MssqlResult.class);

    public static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();

    private final String sql;

    private final ConnectionContext context;

    private final Codecs codecs;

    private final Flux<io.r2dbc.mssql.message.Message> messages;

    private volatile MssqlRowMetadata rowMetadata;

    private volatile RuntimeException throwable;

    private MssqlResult(String sql, ConnectionContext context, Codecs codecs, Flux<io.r2dbc.mssql.message.Message> messages) {

        this.sql = sql;
        this.context = context;
        this.codecs = codecs;
        this.messages = messages;
    }

    /**
     * Create a {@link MssqlResult}.
     *
     * @param sql      the underlying SQL statement.
     * @param codecs   the codecs to use.
     * @param messages message stream.
     * @return {@link Result} object.
     */
    static MssqlResult toResult(String sql, ConnectionContext context, Codecs codecs, Flux<io.r2dbc.mssql.message.Message> messages) {

        Assert.requireNonNull(sql, "SQL must not be null");
        Assert.requireNonNull(codecs, "Codecs must not be null");
        Assert.requireNonNull(context, "ConnectionContext must not be null");
        Assert.requireNonNull(messages, "Messages must not be null");

        LOGGER.debug(context.getMessage("Creating new result"));

        return new MssqlResult(sql, context, codecs, messages);
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
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {

        Assert.requireNonNull(f, "Mapping function must not be null");

        return this.messages
            .<T>handle((message, sink) -> {

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

                if (message.getClass() == RowToken.class || message.getClass() == NbcRowToken.class) {

                    MssqlRowMetadata rowMetadata = this.rowMetadata;

                    if (rowMetadata == null) {
                        sink.error(new IllegalStateException("No MssqlRowMetadata available"));
                        return;
                    }

                    MssqlRow row = MssqlRow.toRow(this.codecs, (RowToken) message, rowMetadata);
                    try {
                        sink.next(f.apply(row, row.getMetadata()));
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

                ReferenceCountUtil.release(message);
            }).doOnComplete(() -> {
                RuntimeException exception = this.throwable;
                if (exception != null) {
                    throw exception;
                }
            });
    }

    @Override
    public Result filter(Predicate<Segment> filter) {

        Flux<io.r2dbc.mssql.message.Message> filteredMessages = this.messages.filter(message -> {

            if (message.getClass() == ColumnMetadataToken.class) {

                ColumnMetadataToken token = (ColumnMetadataToken) message;

                if (token.hasColumns()) {
                    this.rowMetadata = MssqlRowMetadata.create(this.codecs, token);
                }
                return true;
            }

            if (message.getClass() == RowToken.class || message.getClass() == NbcRowToken.class) {

                MssqlRowMetadata rowMetadata = this.rowMetadata;

                if (rowMetadata == null) {
                    return false;
                }

                MssqlRow row = MssqlRow.toRow(this.codecs, (RowToken) message, rowMetadata);

                boolean result = filter.test(row);

                if (!result) {
                    row.release();
                }

                return result;
            }

            if (message instanceof AbstractInfoToken) {
                return filter.test(createMessage((AbstractInfoToken) message));
            }

            if (message instanceof AbstractDoneToken) {

                AbstractDoneToken doneToken = (AbstractDoneToken) message;
                if (doneToken.hasCount()) {

                    return filter.test(doneToken);
                }
            }
            return true;
        });

        return new MssqlResult(this.sql, this.context, this.codecs, filteredMessages);
    }

    @Override
    public <T> Flux<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {

        return this.messages
            .flatMap(message -> {

                if (message instanceof AbstractDoneToken) {

                    AbstractDoneToken doneToken = (AbstractDoneToken) message;
                    if (doneToken.hasCount()) {

                        if (DEBUG_ENABLED) {
                            LOGGER.debug(this.context.getMessage("Incoming row count: {}"), doneToken);
                        }

                        return mappingFunction.apply(doneToken);
                    }
                }

                if (message.getClass() == ColumnMetadataToken.class) {

                    ColumnMetadataToken token = (ColumnMetadataToken) message;

                    if (!token.hasColumns()) {
                        return Mono.empty();
                    }

                    if (DEBUG_ENABLED) {
                        LOGGER.debug(this.context.getMessage("Result column definition: {}"), message);
                    }

                    this.rowMetadata = MssqlRowMetadata.create(this.codecs, token);
                }

                if (message.getClass() == RowToken.class || message.getClass() == NbcRowToken.class) {

                    MssqlRowMetadata rowMetadata1 = this.rowMetadata;

                    if (rowMetadata1 == null) {
                        return Mono.error(new IllegalStateException("No MssqlRowMetadata available"));
                    }

                    MssqlRow row = MssqlRow.toRow(this.codecs, (RowToken) message, rowMetadata1);

                    try {
                        return Flux.from(mappingFunction.apply(row)).doFinally(it -> row.release());
                    } catch (RuntimeException e) {
                        row.release();
                        throw e;
                    }
                }

                if (message instanceof AbstractInfoToken) {
                    return mappingFunction.apply(createMessage((AbstractInfoToken) message));
                }

                ReferenceCountUtil.release(message);

                return Mono.empty();
            });
    }

    private Message createMessage(AbstractInfoToken message) {

        ErrorDetails errorDetails = ExceptionFactory.createErrorDetails(message);

        return new Message() {

            @Override
            public R2dbcException exception() {
                return ExceptionFactory.createException(message, MssqlResult.this.sql);
            }

            @Override
            public int errorCode() {
                return (int) errorDetails.getNumber();
            }

            @Override
            public String sqlState() {
                return errorDetails.getStateCode();
            }

            @Override
            public String message() {
                return errorDetails.getMessage();
            }

            @Override
            public Severity severity() {
                return message instanceof ErrorToken ? Severity.ERROR : Severity.INFO;
            }
        };
    }

}
