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
import io.netty.buffer.PooledByteBufAllocator;
import io.r2dbc.mssql.util.Concurrency;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrency tests for {@link PlpEncoded.ChunkSubscriber}.
 */
class PlpEncodedChunkSubscriberUnitTests {

    // ~5 MB of payload per iteration, fed as many components, to mirror a large NVARCHAR(MAX)
    private static final int COMPONENTS = 2000;

    private static final int COMPONENT_SIZE = 2600; // 2000 * 2600 B = ~5 MiB

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
    void cancelRacingOnNextShouldBeSafe(@Concurrency(2) ExecutorService pool) throws Exception {

        ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

        List<ByteBuf> fed = preallocate(allocator);
        AtomicInteger fedCount = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        CoreSubscriber<ByteBuf> wire = new BaseCoreSubscriber<ByteBuf>() {

            @Override
            public void onNext(ByteBuf chunk) {
                chunk.release();
            }

        };

        PlpEncoded.ChunkSubscriber subscriber = new PlpEncoded.ChunkSubscriber(wire, allocator, () -> COMPONENT_SIZE, false);
        subscriber.onSubscribe(NOOP_UPSTREAM);

        CyclicBarrier start = new CyclicBarrier(2);

        Future<?> feeder = pool.submit(() -> {
            try {
                start.await();
                for (ByteBuf component : fed) {
                    fedCount.incrementAndGet();
                    subscriber.onNext(component);
                }
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        });

        Future<?> canceller = pool.submit(() -> {
            try {
                start.await();

                while (fedCount.get() < CANCEL_AFTER && failure.get() == null
                        && !Thread.currentThread().isInterrupted()) {
                    // busy-spin on the volatile counter until the aggregator is large
                }
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
                subscriber.cancel();
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        });

        feeder.get(30, TimeUnit.SECONDS);
        canceller.get(30, TimeUnit.SECONDS);

        assertThat(failure.get()).describedAs("cancel raced onNext on the aggregator").isNull();

        for (ByteBuf component : fed) {
            assertThat(component.refCnt()).describedAs("component leaked: cancel raced onNext").isZero();
        }
    }

    @RepeatedTest(200)
    void requestRacingOnNextNeitherCorruptsNorLeaks(@Concurrency(2) ExecutorService pool) throws Exception {

        ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

        List<ByteBuf> fed = preallocate(allocator);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        CoreSubscriber<ByteBuf> wire = new BaseCoreSubscriber<ByteBuf>() {

            @Override
            public void onNext(ByteBuf chunk) {
                chunk.release();
            }

            @Override
            public void onError(Throwable t) {
                failure.compareAndSet(null, t);
            }

        };

        PlpEncoded.ChunkSubscriber subscriber = new PlpEncoded.ChunkSubscriber(wire, allocator, () -> COMPONENT_SIZE, false);
        subscriber.onSubscribe(NOOP_UPSTREAM);

        CyclicBarrier start = new CyclicBarrier(2);

        Future<?> feeder = pool.submit(() -> {
            try {
                start.await();
                for (ByteBuf component : fed) {
                    subscriber.onNext(component);
                }
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        });

        Future<?> requester = pool.submit(() -> {
            try {
                start.await();
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
    }

    @Test
    void doesNotEmitAfterDownstreamCancelsMidDrain() {

        ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

        AtomicInteger emitted = new AtomicInteger();
        List<ByteBuf> received = new ArrayList<>();
        AtomicReference<Subscription> downstreamSubscription = new AtomicReference<>();

        CoreSubscriber<ByteBuf> cancelsOnFirst = new BaseCoreSubscriber<ByteBuf>() {

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

    /**
     * Pre-allocate so the feeder thread spends ~all its time inside onNext
     */
    private List<ByteBuf> preallocate(ByteBufAllocator allocator) {
        List<ByteBuf> fed = new ArrayList<>(COMPONENTS);
        for (int i = 0; i < COMPONENTS; i++) {
            fed.add(allocator.buffer(COMPONENT_SIZE).writeZero(COMPONENT_SIZE));
        }
        return fed;
    }

    static class BaseCoreSubscriber<T> implements CoreSubscriber<T> {

        @Override
        public void onSubscribe(Subscription s) {
        }

        @Override
        public void onNext(T t) {
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onComplete() {
        }

    }

}
