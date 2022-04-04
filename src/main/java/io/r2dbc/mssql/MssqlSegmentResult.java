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

package io.r2dbc.mssql;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
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
import io.r2dbc.spi.OutParameters;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * {@link Result} of query results.
 *
 * @author Mark Paluch
 * @since 0.9
 */
final class MssqlSegmentResult implements MssqlResult {

    private static final Logger LOGGER = Loggers.getLogger(MssqlSegmentResult.class);

    public static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();

    private final String sql;

    private final ConnectionContext context;

    private final Codecs codecs;

    private final Flux<Segment> segments;

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
    static MssqlSegmentResult toResult(String sql, ConnectionContext context, Codecs codecs, Flux<io.r2dbc.mssql.message.Message> messages, boolean expectReturnValues) {

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

            List<ReturnValue> returnValues = new ArrayList<>();

            messageStream = messageStream.doOnNext(message -> {

                if (message instanceof ReturnValue) {
                    returnValues.add((ReturnValue) message);
                }
            }).filter(it -> !(it instanceof ReturnValue));

            returnValueStream = Flux.defer(() -> {

                if (returnValues.size() != 0) {
                    return Flux.just(new MsqlOutSegment(codecs, returnValues));
                }

                return Flux.empty();
            });
        }

        AtomicReference<MssqlRowMetadata> metadataRef = new AtomicReference<>();
        Flux<Segment> segments = messageStream.handle((message, sink) -> {

            if (message instanceof AbstractDoneToken) {

                AbstractDoneToken doneToken = (AbstractDoneToken) message;
                if (doneToken.isAttentionAck()) {
                    sink.error(new ExceptionFactory.MssqlStatementCancelled(sql));
                    return;
                }
            }

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

                sink.next(new MssqlRowSegment(codecs, (RowToken) message, rowMetadata));
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

            ReferenceCountUtil.release(message);
        });

        if (expectReturnValues) {
            segments = segments.concatWith(returnValueStream);
        }

        return segments;
    }

    @Override
    public Mono<Long> getRowsUpdated() {

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
                    sink.error(((Message) segment).exception());
                    return;
                }

                ReferenceCountUtil.release(segment);
            }).reduce(Long::sum);
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
    public <T> Flux<T> map(Function<? super Readable, ? extends T> mappingFunction) {

        Assert.requireNonNull(mappingFunction, "Mapping function must not be null");

        return doMap(true, true, mappingFunction);
    }

    private <T> Flux<T> doMap(boolean rows, boolean outparameters, Function<? super Readable, ? extends T> mappingFunction) {

        return this.segments
            .handle((segment, sink) -> {

                if (rows && segment instanceof RowSegment) {

                    RowSegment data = (RowSegment) segment;
                    Row row = data.row();
                    try {
                        sink.next(mappingFunction.apply(row));
                    } finally {
                        ReferenceCountUtil.release(data);
                    }

                    return;
                }

                if (outparameters && segment instanceof OutSegment) {

                    OutSegment data = (OutSegment) segment;
                    OutParameters outParameters = data.outParameters();
                    try {
                        sink.next(mappingFunction.apply(outParameters));
                    } finally {
                        ReferenceCountUtil.release(data);
                    }

                    return;
                }

                if (isError(segment)) {
                    sink.error(((Message) segment).exception());
                    return;
                }

                ReferenceCountUtil.release(segment);
            });
    }

    @Override
    public MssqlResult filter(Predicate<Segment> filter) {

        Assert.requireNonNull(filter, "filter must not be null");

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
    @SuppressWarnings("unchecked")
    public <T> Flux<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {

        Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");

        return this.segments
            .concatMap(segment -> {

                Publisher<? extends T> result = mappingFunction.apply(segment);

                if (result == null) {
                    return Mono.error(new IllegalStateException("The mapper returned a null Publisher"));
                }

                // doAfterTerminate to not release resources before they had a chance to get emitted
                if (result instanceof Mono) {
                    return ((Mono<T>) result).doFinally(s -> ReferenceCountUtil.release(segment));
                }

                return Flux.from(result).doFinally(s -> ReferenceCountUtil.release(segment));
            });
    }

    private boolean isError(Segment segment) {
        return segment instanceof MssqlMessage && ((MssqlMessage) segment).isError();
    }

    private static Message createMessage(String sql, AbstractInfoToken message) {

        ErrorDetails errorDetails = ExceptionFactory.createErrorDetails(message);

        return new MssqlMessage(message, sql, errorDetails);
    }

    static class MssqlMessage implements Message {

        private final ErrorDetails errorDetails;

        private final AbstractInfoToken message;

        private final String sql;

        public MssqlMessage(AbstractInfoToken message, String sql, ErrorDetails errorDetails) {
            this.message = message;
            this.sql = sql;
            this.errorDetails = errorDetails;
        }

        @Override
        public R2dbcException exception() {
            return ExceptionFactory.createException(this.message, this.sql);
        }

        @Override
        public int errorCode() {
            return (int) this.errorDetails.getNumber();
        }

        @Override
        public String sqlState() {
            return this.errorDetails.getStateCode();
        }

        @Override
        public String message() {
            return this.errorDetails.getMessage();
        }

        public boolean isError() {
            return this.message instanceof ErrorToken;
        }

    }

    private static class MssqlRowSegment extends AbstractReferenceCounted implements RowSegment {

        private final RowToken rowToken;

        private final MssqlRow row;

        public MssqlRowSegment(Codecs codecs, RowToken rowToken, MssqlRowMetadata rowMetadata) {
            this.rowToken = rowToken;
            this.row = MssqlRow.toRow(codecs, this.rowToken, rowMetadata);
        }

        @Override
        public Row row() {
            return this.row;
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            return this;
        }

        @Override
        protected void deallocate() {
            this.rowToken.release();
        }

    }

    private static class MsqlOutSegment extends AbstractReferenceCounted implements OutSegment {

        private final MssqlReturnValues returnValues;

        public MsqlOutSegment(Codecs codecs, List<ReturnValue> returnValues) {
            this.returnValues = MssqlReturnValues.toReturnValues(codecs, returnValues);
        }

        @Override
        public OutParameters outParameters() {
            return this.returnValues;
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            return this;
        }

        @Override
        protected void deallocate() {
            this.returnValues.release();
        }

    }

}
