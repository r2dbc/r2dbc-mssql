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
 * Concurrency regression tests for {@link PlpEncoded.ChunkSubscriber}. {@code onNext} hands its buffer to a
 * work-in-progress-serialised drain that exclusively owns the {@code CompositeByteBuf} aggregator, so the
 * aggregator is never mutated/released by two threads at once. Both downstream signals can race {@code onNext}
 * (upstream delivery): {@code cancel} (which releases the aggregator) and {@code request} (which drives a second
 * concurrent {@code drain()}). On the unserialised code each fails ~50% of runs — a {@code cancel} releasing the
 * aggregator mid-{@code addComponent} throws {@code refCnt: 0}, and a concurrent {@code request}/{@code onNext}
 * drain corrupts the aggregator's reader/writer indices ({@code IndexOutOfBoundsException}) — or leaks a chunk.
 * Both must instead release every fed component exactly once with no exception.
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

    // No cancel: request() (downstream demand) races onNext() (upstream delivery), driving two concurrent
    // drain()s over the non-thread-safe CompositeByteBuf. On the unserialised code this fails ~50% of runs
    // (refCnt: 0 / reader-writer index corruption), proving the defect is not specific to cancellation.
    @RepeatedTest(200)
    void requestRacingOnNextNeitherCorruptsNorLeaks() throws Exception {

        ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        ExecutorService pool = Executors.newFixedThreadPool(2);

        try {
            List<ByteBuf> fed = new ArrayList<>(COMPONENTS);
            for (int i = 0; i < COMPONENTS; i++) {
                fed.add(allocator.buffer(COMPONENT_SIZE).writeZero(COMPONENT_SIZE));
            }
            AtomicReference<Throwable> failure = new AtomicReference<>();

            CoreSubscriber<ByteBuf> wire = new CoreSubscriber<ByteBuf>() {
                @Override
                public Context currentContext() {
                    return Context.empty();
                }

                @Override
                public void onSubscribe(Subscription s) {
                }

                @Override
                public void onNext(ByteBuf chunk) {
                    chunk.release();
                }

                @Override
                public void onError(Throwable t) {
                    failure.compareAndSet(null, t);
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
                    for (ByteBuf component : fed) {
                        subscriber.onNext(component);
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });

            Future<?> requester = pool.submit(() -> {
                awaitQuietly(start);
                try {
                    for (int i = 0; i < COMPONENTS; i++) {
                        subscriber.request(1);
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });

            feeder.get(30, TimeUnit.SECONDS);
            requester.get(30, TimeUnit.SECONDS);

            // Drain any remainder and complete; every fed component must be released exactly once.
            subscriber.request(Long.MAX_VALUE);
            subscriber.onComplete();

            assertThat(failure.get()).describedAs("request raced onNext on the aggregator (no cancel)").isNull();
            for (ByteBuf component : fed) {
                assertThat(component.refCnt()).describedAs("component leaked: request raced onNext").isZero();
            }
        } finally {
            pool.shutdownNow();
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
