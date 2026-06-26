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
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrency regression test for {@link PlpEncoded.ChunkSubscriber}. Incoming chunks are appended to a
 * {@code CompositeByteBuf} aggregator while {@code cancel}/{@code onError} release it. {@code onNext} hands its
 * buffer to a work-in-progress-serialised drain that exclusively owns the aggregator, so an append can never run
 * concurrently with a release. This test reproduces a &gt;5&nbsp;MB NVARCHAR(MAX)-sized stream fed on one thread
 * while the subscription is cancelled on another: every fed component must end up released exactly once, with no
 * {@code IllegalReferenceCountException} and no leak. Without the serialisation, a cancel that releases the
 * aggregator mid-{@code addComponent} fails with {@code refCnt: 0} or leaks the component.
 */
class PlpEncodedChunkSubscriberConcurrencyUnitTests {

    // ~5 MB of payload per iteration, fed as many components, to mirror a large NVARCHAR(MAX) bind and to
    // widen the cancel-vs-onNext window so the race is hit reliably when the guard is absent.
    private static final int COMPONENTS = 2000;

    private static final int COMPONENT_SIZE = 2600; // 2000 * 2600 B = ~5 MiB

    // Cancel once this many components are buffered: the aggregator is large, so its (component-by-component)
    // release overlaps the in-flight onNext addComponent.
    private static final int CANCEL_AFTER = 1000;

    private static final Subscription NOOP_UPSTREAM = new Subscription() {
        @Override
        public void request(long n) {
        }

        @Override
        public void cancel() {
        }
    };

    @RepeatedTest(200)
    void cancelRacingOnNextNeitherLeaksNorDoubleFreesTheAggregator() throws Exception {

        ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        ExecutorService pool = Executors.newFixedThreadPool(2);

        try {
            // Pre-allocate so the feeder thread spends ~all its time inside onNext (not allocating),
            // which is what makes a mid-feed cancel actually overlap an in-flight addComponent.
            List<ByteBuf> fed = new ArrayList<>(COMPONENTS);
            for (int i = 0; i < COMPONENTS; i++) {
                fed.add(allocator.buffer(COMPONENT_SIZE).writeZero(COMPONENT_SIZE));
            }
            AtomicInteger fedCount = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();

            // Stands in for the wire encoder: consumes (releases) any chunk that is emitted.
            CoreSubscriber<ByteBuf> wire = new CoreSubscriber<ByteBuf>() {
                @Override
                public Context currentContext() {
                    return Context.empty();
                }

                @Override
                public void onSubscribe(Subscription s) {
                    // Do not request: leaving the chunks buffered in the aggregator widens the window in which
                    // a concurrent cancel/release races onNext's addComponent.
                }

                @Override
                public void onNext(ByteBuf chunk) {
                    chunk.release();
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onComplete() {
                }
            };

            PlpEncoded.ChunkSubscriber subscriber = new PlpEncoded.ChunkSubscriber(wire, allocator, () -> COMPONENT_SIZE, false);
            subscriber.onSubscribe(NOOP_UPSTREAM);

            CyclicBarrier start = new CyclicBarrier(2);

            Future<?> feeder = pool.submit(() -> {
                awaitQuietly(start);
                try {
                    // Feed the whole (pre-allocated) stream. We do NOT call onComplete: a cancelled stream's
                    // upstream is cancelled and never completes. Every fed buffer is still freed — drained to the
                    // wire, or released by the serialised drain when it discards the queue/aggregator after cancel.
                    for (ByteBuf component : fed) {
                        fedCount.incrementAndGet();
                        subscriber.onNext(component);
                    }
                } catch (Throwable t) {
                    // e.g. IllegalReferenceCountException when the guard is removed (release raced addComponent).
                    failure.compareAndSet(null, t);
                }
            });

            Future<?> canceller = pool.submit(() -> {
                awaitQuietly(start);
                while (fedCount.get() < CANCEL_AFTER && failure.get() == null) {
                    // busy-spin on the volatile counter until the aggregator is large
                }
                try {
                    subscriber.cancel();
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });

            feeder.get(30, TimeUnit.SECONDS);
            canceller.get(30, TimeUnit.SECONDS);

            assertThat(failure.get()).describedAs("cancel raced onNext on the aggregator").isNull();

            // Every fed component must end up released — either drained to the wire, or freed when the
            // aggregator is released on cancel/complete. A non-zero refCnt is a leaked component (the
            // race added it after the aggregator was released).
            for (ByteBuf component : fed) {
                assertThat(component.refCnt()).describedAs("component leaked: cancel raced onNext").isZero();
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // A downstream that cancels synchronously from its first onNext (like take(1)) must not receive any
    // further onNext, and the buffered remainder must be released (no leak). Single-threaded and deterministic.
    @Test
    void doesNotEmitAfterDownstreamCancelsMidDrain() {

        ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

        AtomicInteger emitted = new AtomicInteger();
        List<ByteBuf> received = new ArrayList<>();
        AtomicReference<Subscription> downstreamSubscription = new AtomicReference<>();

        CoreSubscriber<ByteBuf> cancelsOnFirst = new CoreSubscriber<ByteBuf>() {
            @Override
            public Context currentContext() {
                return Context.empty();
            }

            @Override
            public void onSubscribe(Subscription s) {
                downstreamSubscription.set(s);
                s.request(5);
            }

            @Override
            public void onNext(ByteBuf chunk) {
                emitted.incrementAndGet();
                received.add(chunk);
                downstreamSubscription.get().cancel(); // cancel synchronously from within onNext
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        };

        // chunk size 10, feed 25 bytes -> two full chunks are available in a single drain.
        PlpEncoded.ChunkSubscriber subscriber = new PlpEncoded.ChunkSubscriber(cancelsOnFirst, allocator, () -> 10, false);
        subscriber.onSubscribe(NOOP_UPSTREAM);

        ByteBuf data = allocator.buffer(25).writeZero(25);
        subscriber.onNext(data);

        try {
            assertThat(emitted.get()).describedAs("must not emit another chunk after a synchronous cancel").isEqualTo(1);
            assertThat(data.refCnt()).describedAs("buffered remainder released after cancel").isZero();
        } finally {
            received.forEach(buf -> {
                if (buf.refCnt() > 0) {
                    buf.release();
                }
            });
        }
    }

    private static void awaitQuietly(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
