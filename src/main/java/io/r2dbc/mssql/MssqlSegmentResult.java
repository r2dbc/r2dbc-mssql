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
import io.r2dbc.mssql.message.token.ReturnValue;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * {@link Result} of query results.
 *
 * @author Mark Paluch
 */
final class MssqlSegmentResult implements MssqlResult {

    private static final Logger LOGGER = Loggers.getLogger(MssqlSegmentResult.class);

    public static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();

    private final String sql;

    private final ConnectionContext context;

    private final Codecs codecs;

    private final Flux<Segment> segments;

    private volatile RuntimeException throwable;

    private MssqlSegmentResult(String sql, ConnectionContext context, Codecs codecs, Flux<Segment> segments) {

        this.sql = sql;
        this.context = context;
        this.codecs = codecs;
        this.segments = segments;
    }

    /**
     * Create a {@link MssqlSegmentResult}.
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

        return new MssqlSegmentResult(sql, context, codecs, toSegments(sql, codecs, messages, expectReturnValues));
    }

    private static Flux<Segment> toSegments(String sql, Codecs codecs, Flux<io.r2dbc.mssql.message.Message> messages, boolean expectReturnValues) {

        Flux<Segment> returnValueStream = Flux.empty();
        Flux<io.r2dbc.mssql.message.Message> messageStream = messages;

        if (expectReturnValues) {

            final List<ReturnValue> returnValues = new ArrayList<>();

            messageStream = messageStream.doOnNext(message -> {

                if (message instanceof ReturnValue) {
                    returnValues.add((ReturnValue) message);
                }
            });

            returnValueStream = Flux.defer(() -> {

                if (returnValues.size() != 0) {
                    return Flux.just(MssqlReturnValues.toReturnValues(codecs, returnValues));
                }

                return Flux.empty();
            });
        }

        AtomicReference<MssqlRowMetadata> metadataRef = new AtomicReference<>();
        Flux<Segment> segments = messageStream.handle((message, sink) -> {

            if (message.getClass() == ColumnMetadataToken.class) {

                ColumnMetadataToken token = (ColumnMetadataToken) message;

                if (token.hasColumns()) {
                    metadataRef.set(MssqlRowMetadata.create(codecs, token));
                }
                return;
            }

            if (message.getClass() == RowToken.class || message.getClass() == NbcRowToken.class) {

                MssqlRowMetadata rowMetadata = metadataRef.get();

                if (rowMetadata == null) {
                    return;
                }

                sink.next(MssqlRow.toRow(codecs, (RowToken) message, rowMetadata));
                return;
            }

            if (message instanceof AbstractInfoToken) {
                sink.next(createMessage(sql, (AbstractInfoToken) message));
                return;
            }

            if (message instanceof AbstractDoneToken) {

                AbstractDoneToken doneToken = (AbstractDoneToken) message;
                if (doneToken.hasCount()) {

                    sink.next(doneToken);
                }
            }
        });

        if (expectReturnValues) {
            segments = segments.concatWith(returnValueStream);
        }

        return segments;
    }

    @Override
    public Mono<Integer> getRowsUpdated() {

        return this.segments
            .<Long>handle((segment, sink) -> {

                if (segment instanceof UpdateCount) {

                    UpdateCount updateCount = (UpdateCount) segment;

                    if (DEBUG_ENABLED) {
                        LOGGER.debug(this.context.getMessage("Incoming row count: {}"), updateCount);
                    }

                    sink.next(updateCount.value());
                }

                if (isError(segment)) {
                    handleError((Message) segment);
                    return;
                }

                ReferenceCountUtil.release(segment);
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

        return this.segments
            .<T>handle((segment, sink) -> {

                if (segment instanceof Data) {

                    Data data = (Data) segment;
                    try {
                        sink.next(f.apply(data, data.metadata()));
                    } finally {
                        ReferenceCountUtil.release(data);
                    }

                    return;
                }

                if (isError(segment)) {
                    handleError((Message) segment);
                    return;
                }

                ReferenceCountUtil.release(segment);
            }).doOnComplete(() -> {
                RuntimeException exception = this.throwable;
                if (exception != null) {
                    throw exception;
                }
            });
    }

    private void handleError(Message segment) {
        R2dbcException mssqlException = segment.exception();

        Throwable exception = this.throwable;
        if (exception != null) {
            exception.addSuppressed(mssqlException);
        } else {
            this.throwable = mssqlException;
        }
    }

    private boolean isError(Segment segment) {
        return segment instanceof Message && ((Message) segment).severity() == Message.Severity.ERROR;
    }

    @Override
    public Result filter(Predicate<Segment> filter) {

        Flux<Segment> filteredSegments = this.segments.filter(message -> {

            if (filter.test(message)) {
                return true;
            }

            ReferenceCountUtil.release(message);
            return false;
        });

        return new MssqlSegmentResult(this.sql, this.context, this.codecs, filteredSegments);
    }

    @Override
    public <T> Flux<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {

        return this.segments
            .flatMap(segment -> {

                try {
                    Publisher<? extends T> publisher = mappingFunction.apply(segment);

                    if (publisher instanceof Mono) {
                        return ((Mono<T>) publisher).doFinally(it -> ReferenceCountUtil.release(segment));
                    }

                    return Flux.from(publisher).doFinally(it -> ReferenceCountUtil.release(segment));
                } catch (RuntimeException e) {
                    ReferenceCountUtil.release(segment);
                    throw e;
                }
            });
    }

    private static Message createMessage(String sql, AbstractInfoToken message) {

        ErrorDetails errorDetails = ExceptionFactory.createErrorDetails(message);

        return new Message() {

            @Override
            public R2dbcException exception() {
                return ExceptionFactory.createException(message, sql);
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
