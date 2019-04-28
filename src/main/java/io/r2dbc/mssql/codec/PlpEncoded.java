/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.SqlServerType;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.IntSupplier;

/**
 * Partial-length-prefixed extension to {@link Encoded}. Consumes a upstream {@link Publisher}.
 *
 * @author Mark Paluch
 */
public class PlpEncoded extends Encoded {

    private final SqlServerType serverType;

    private final ByteBufAllocator allocator;

    private final Publisher<ByteBuf> dataStream;

    private final Disposable disposable;

    public PlpEncoded(SqlServerType dataType, ByteBufAllocator allocator, Publisher<ByteBuf> dataStream, Disposable disposable) {

        super(dataType.getNullableType(), Unpooled.EMPTY_BUFFER);

        this.serverType = dataType;
        this.allocator = allocator;
        this.dataStream = dataStream;
        this.disposable = disposable;
    }

    public void encodeHeader(ByteBuf byteBuf) {

        // Send v*max length indicator 0xFFFF.
        Encode.uShort(byteBuf, Length.USHORT_NULL);
    }

    @Override
    public PlpEncoded touch(Object hint) {
        return this;
    }

    @Override
    protected void deallocate() {
        this.disposable.dispose();
    }

    /**
     * Transform the backing binary stream to a stream of binary chunks at the size provided by {@link IntSupplier chunk size supplier}.
     *
     * @param chunkSize expected chunk size.
     * @return
     */
    public Flux<ByteBuf> chunked(IntSupplier chunkSize) {
        return chunked(chunkSize, false);
    }

    /**
     * Transform the backing binary stream to a stream of binary chunks at the size provided by {@link IntSupplier chunk size supplier}.
     *
     * @param chunkSize       expected chunk size.
     * @param withSizeHeaders {@literal true} to include PLP length headers (one unknown length and chunk length per chunk).
     * @return
     */
    public Flux<ByteBuf> chunked(IntSupplier chunkSize, boolean withSizeHeaders) {
        return new ChunkOperator(Flux.from(this.dataStream), this.allocator, chunkSize, withSizeHeaders);
    }

    @Override
    public String getFormalType() {

        switch (this.serverType) {
            case VARBINARYMAX:
                return "VARBINARY(MAX)";
            case VARCHARMAX:
                return "VARCHAR(MAX)";
            case NVARCHARMAX:
                return "NVARCHAR(MAX)";
        }

        throw new UnsupportedOperationException("Type " + this.serverType + " not supported");
    }

    /**
     * Operator for chunked encoding of {@link ByteBuf}. Chunk size is obtained from {@link IntSupplier} on
     */
    static class ChunkOperator extends FluxOperator<ByteBuf, ByteBuf> {

        private final ByteBufAllocator allocator;

        private final IntSupplier chunkSizeSupplier;

        private final boolean withSizeHeaders;

        ChunkOperator(Flux<ByteBuf> source, ByteBufAllocator allocator, IntSupplier chunkSizeSupplier, boolean withSizeHeaders) {

            super(source);
            this.allocator = allocator;
            this.chunkSizeSupplier = chunkSizeSupplier;
            this.withSizeHeaders = withSizeHeaders;
        }

        @Override
        public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
            this.source.subscribe(new ChunkSubscriber(actual, this.allocator, this.chunkSizeSupplier, this.withSizeHeaders));
        }
    }

    static class ChunkSubscriber extends AtomicLong implements CoreSubscriber<ByteBuf>, Subscription {

        private static final int STATUS_WIP = 0;

        private static final int STATUS_DONE = 1;

        private final CoreSubscriber<? super ByteBuf> actual;

        private final ByteBufAllocator allocator;

        private final IntSupplier chunkSizeSupplier;

        private final boolean withSizeHeaders;

        private boolean first = true;

        private volatile int nextChunkSize;

        @Nullable
        private volatile CompositeByteBuf aggregator;

        volatile long requested;

        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<ChunkSubscriber> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(ChunkSubscriber.class, "requested");

        volatile int status;

        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ChunkSubscriber> STATUS =
            AtomicIntegerFieldUpdater.newUpdater(ChunkSubscriber.class, "status");

        private boolean doneUpstream;

        private Subscription s;

        ChunkSubscriber(CoreSubscriber<? super ByteBuf> actual, ByteBufAllocator allocator, IntSupplier chunkSizeSupplier, boolean withSizeHeaders) {

            this.actual = actual;
            this.allocator = allocator;
            this.chunkSizeSupplier = chunkSizeSupplier;
            this.withSizeHeaders = withSizeHeaders;
        }

        @Override
        public Context currentContext() {
            return this.actual.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {

            if (Operators.validate(this.s, s)) {
                this.s = s;
                this.actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(ByteBuf byteBuf) {

            if (STATUS.get(this) == STATUS_DONE) {

                byteBuf.release();
                Operators.onNextDropped(byteBuf, this.actual.currentContext());
                return;
            }

            CompositeByteBuf aggregator = this.aggregator;
            if (aggregator == null) {
                this.aggregator = aggregator = this.allocator.compositeBuffer();
            }

            aggregator.addComponent(true, byteBuf);

            drain();

            if (!this.doneUpstream && REQUESTED.get(this) > 0) {
                this.s.request(1);
            }
        }

        private void drain() {

            CompositeByteBuf aggregator = this.aggregator;

            if (aggregator == null) {

                if (this.doneUpstream && STATUS.compareAndSet(this, STATUS_WIP, STATUS_DONE)) {
                    this.actual.onComplete();
                }

                return;
            }

            while (STATUS.get(this) == STATUS_WIP && aggregator.readableBytes() >= this.nextChunkSize && REQUESTED.get(this) > 0) {

                long demand = REQUESTED.get(this);
                if (demand > 0) {
                    if (REQUESTED.compareAndSet(this, demand, demand - 1)) {
                        emitNext(aggregator, this.nextChunkSize);
                        this.nextChunkSize = this.chunkSizeSupplier.getAsInt();
                    }
                }
            }

            if (STATUS.get(this) == STATUS_WIP && this.doneUpstream && aggregator.isReadable() && REQUESTED.get(this) > 0) {

                long demand = REQUESTED.get(this);
                if (demand > 0) {
                    if (REQUESTED.compareAndSet(this, demand, demand - 1)) {
                        emitNext(aggregator, aggregator.readableBytes());
                    }
                }
            }

            if (this.doneUpstream && !aggregator.isReadable() && STATUS.compareAndSet(this, STATUS_WIP, STATUS_DONE)) {

                aggregator.release();

                this.aggregator = null;
                this.actual.onComplete();
            }
        }

        private void emitNext(CompositeByteBuf aggregator, int bytesToRead) {

            ByteBuf buffer = aggregator.readRetainedSlice(bytesToRead);

            if (this.withSizeHeaders) {

                CompositeByteBuf composite = this.allocator.compositeBuffer();

                ByteBuf header = this.allocator.buffer();
                if (this.first) {

                    this.first = false;
                    PlpLength.unknown().encode(header);
                }

                Length chunkLength = Length.of(bytesToRead);
                chunkLength.encode(header, LengthStrategy.PARTLENTYPE);

                composite.addComponent(true, header);
                composite.addComponent(true, buffer);

                buffer = composite;
            }

            this.actual.onNext(buffer);
        }

        @Override
        public void onError(Throwable t) {

            CompositeByteBuf aggregator = this.aggregator;

            if (STATUS.compareAndSet(this, STATUS_WIP, STATUS_DONE)) {

                this.doneUpstream = true;

                this.actual.onError(t);


                if (aggregator != null) {
                    aggregator.release();
                }

                return;
            }

            Operators.onErrorDropped(t, this.actual.currentContext());
        }

        @Override
        public void onComplete() {

            this.doneUpstream = true;
            drain();
        }

        @Override
        public void request(long n) {

            if (Operators.validate(n)) {

                Operators.addCap(REQUESTED, this, n);
                drain();

                this.nextChunkSize = this.chunkSizeSupplier.getAsInt();

                if (!this.doneUpstream && REQUESTED.get(this) > 0) {
                    this.s.request(1);
                }
            }
        }

        @Override
        public void cancel() {

            if (!this.doneUpstream) {
                this.doneUpstream = true;
                this.s.cancel();
            }

            if (STATUS.compareAndSet(this, STATUS_WIP, STATUS_DONE)) {

                CompositeByteBuf aggregator = this.aggregator;

                if (aggregator != null) {
                    aggregator.release();
                }
            }
        }
    }
}
