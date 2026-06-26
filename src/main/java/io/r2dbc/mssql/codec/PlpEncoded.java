/*
 * Copyright 2019-2022 the original author or authors.
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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import reactor.util.concurrent.Queues;
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

        super(dataType.getNullableType(), () -> Unpooled.EMPTY_BUFFER);

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
    public boolean isDisposed() {
        return this.disposable.isDisposed();
    }

    @Override
    public void dispose() {
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
     * @param withSizeHeaders {@code true} to include PLP length headers (one unknown length and chunk length per chunk).
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

        private final CoreSubscriber<? super ByteBuf> actual;

        private final ByteBufAllocator allocator;

        private final IntSupplier chunkSizeSupplier;

        private final boolean withSizeHeaders;

        // Incoming raw buffers are handed to the WIP-serialised drain() via this queue instead of being added
        // to the aggregator directly from onNext. Together with the work-in-progress counter (this AtomicLong)
        // this keeps every aggregator mutation/release on a single thread at a time, so a concurrent cancel()
        // or onError() can never release the aggregator while onNext() is appending to it - without a lock.
        private final Queue<ByteBuf> queue = Queues.<ByteBuf>unbounded().get();

        private boolean first = true;

        private volatile int nextChunkSize;

        // Only accessed inside drain(), which the wip counter makes single-threaded at a time.
        @Nullable
        private CompositeByteBuf aggregator;

        private boolean terminated;

        volatile long requested;

        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<ChunkSubscriber> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(ChunkSubscriber.class, "requested");

        private volatile boolean doneUpstream;

        @Nullable
        private volatile Throwable error;

        private volatile boolean cancelled;

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

            byteBuf.touch("PlpEncoded.onNext(…)");

            // Hand the buffer to the serialised drain rather than mutating the aggregator here; drain()
            // releases it if the subscription has already been cancelled or terminated.
            this.queue.offer(byteBuf);

            drain();

            if (!this.doneUpstream && !this.cancelled && REQUESTED.get(this) > 0) {
                this.s.request(1);
            }
        }

        private void drain() {

            if (getAndIncrement() != 0) {
                return;
            }

            long missed = 1;

            for (; ; ) {

                if (this.cancelled) {
                    discardAll();
                } else if (this.error != null) {

                    discardAll();
                    if (!this.terminated) {
                        this.terminated = true;
                        this.actual.onError(this.error);
                    }
                } else {

                    // Ingest queued raw buffers into the aggregator. This is the only place that appends,
                    // and it runs single-threaded under the wip guard, so it never races a release below.
                    ByteBuf next;
                    while ((next = this.queue.poll()) != null) {

                        CompositeByteBuf aggregator = this.aggregator;
                        if (aggregator == null) {
                            this.aggregator = aggregator = this.allocator.compositeBuffer();
                        }
                        aggregator.addComponent(true, next);
                    }

                    CompositeByteBuf aggregator = this.aggregator;

                    if (aggregator != null) {

                        while (!this.cancelled && aggregator.readableBytes() >= this.nextChunkSize && REQUESTED.get(this) > 0) {

                            long demand = REQUESTED.get(this);
                            if (demand > 0 && REQUESTED.compareAndSet(this, demand, demand - 1)) {
                                emitNext(aggregator, this.nextChunkSize);
                                this.nextChunkSize = this.chunkSizeSupplier.getAsInt();
                            }
                        }

                        if (!this.cancelled && this.doneUpstream && aggregator.isReadable() && REQUESTED.get(this) > 0) {

                            long demand = REQUESTED.get(this);
                            if (demand > 0 && REQUESTED.compareAndSet(this, demand, demand - 1)) {
                                emitNext(aggregator, aggregator.readableBytes());
                            }
                        }
                    }

                    // The !cancelled checks below are defensive: they preserve the "no onComplete after a
                    // cancel" guarantee that the previous implementation got from a shared status CAS. Only a
                    // tight concurrent interleaving (a cancel landing between the top-of-loop check and here)
                    // could reach them, so they are not covered by a deterministic test.
                    if (!this.cancelled && this.doneUpstream && this.queue.isEmpty() && (this.aggregator == null || !this.aggregator.isReadable())) {

                        if (this.aggregator != null) {
                            this.aggregator.release();
                            this.aggregator = null;
                        }

                        if (!this.terminated) {
                            this.terminated = true;
                            this.actual.onComplete();
                        }
                    } else if (!this.cancelled && this.aggregator != null) {
                        this.aggregator.discardReadComponents();
                    }
                }

                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        // Release everything we hold (queued-but-not-ingested buffers and the aggregator). Runs only inside
        // the serialised drain, so it never races the ingest above.
        private void discardAll() {

            ByteBuf buf;
            while ((buf = this.queue.poll()) != null) {
                buf.release();
            }

            CompositeByteBuf aggregator = this.aggregator;
            if (aggregator != null) {
                this.aggregator = null;
                aggregator.release();
            }
        }

        /**
         * Emit a chunk from the buffer. At this point we need to make sure that buffers which get emitted do not hold a reference to the aggregator as they might cross thread boundaries.
         *
         * @param aggregator
         * @param bytesToRead
         */
        private void emitNext(CompositeByteBuf aggregator, int bytesToRead) {

            ByteBuf buffer = aggregator.alloc().buffer(bytesToRead);
            buffer.writeBytes(aggregator, bytesToRead);

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

            this.error = t;
            this.doneUpstream = true;
            drain();
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

                if (!this.doneUpstream && !this.cancelled && REQUESTED.get(this) > 0) {
                    this.s.request(1);
                }
            }
        }

        @Override
        public void cancel() {

            this.cancelled = true;

            if (this.s != null) {
                this.s.cancel();
            }

            drain();
        }

    }

}
