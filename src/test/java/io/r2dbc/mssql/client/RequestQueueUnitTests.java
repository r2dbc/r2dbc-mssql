/*
 * Copyright 2026 the original author or authors.
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

package io.r2dbc.mssql.client;

import io.r2dbc.mssql.client.ReactorNettyClient.RequestQueue;
import io.r2dbc.mssql.client.ReactorNettyClient.Sinkable;
import io.r2dbc.mssql.util.Concurrency;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RequestQueue} and its {@link RequestQueue.Lease}, which serialize the request/response windows
 * of a connection.
 *
 * @author Mark Paluch
 */
class RequestQueueUnitTests {

    private final RequestQueue queue = new RequestQueue(new ConnectionContext());

    private final TestExchange first = new TestExchange();

    private final TestExchange second = new TestExchange();

    @Test
    void shouldAdmitFirstExchange() {

        this.queue.submit(this.first);

        assertThat(this.first.isAdmitted()).isTrue();
    }

    @Test
    void shouldQueueExchangeWhileTheWindowIsHeld() {

        this.queue.submit(this.first);
        this.queue.submit(this.second);

        assertThat(this.second.isAdmitted()).isFalse();
    }

    @Test
    void shouldAdmitQueuedExchangeOnRelease() {

        this.queue.submit(this.first);
        this.queue.submit(this.second);

        assertThat(this.first.getLease().release()).isTrue();

        assertThat(this.second.isAdmitted()).isTrue();
        assertThat(this.second.getLease()).isNotSameAs(this.first.getLease());
    }

    @Test
    void shouldReleaseWindowOnlyOnce() {

        TestExchange third = new TestExchange();

        this.queue.submit(this.first);
        this.queue.submit(this.second);
        this.queue.submit(third);

        this.first.getLease().release();

        assertThat(this.first.getLease().release()).isFalse();
        assertThat(third.isAdmitted()).isFalse();
    }

    @Test
    void shouldAdmitNextExchangeAfterAnIdleRelease() {

        this.queue.submit(this.first);
        this.first.getLease().release();

        this.queue.submit(this.second);

        assertThat(this.second.isAdmitted()).isTrue();
    }

    @Test
    void shouldNotStrandSubmissionRacingIdleRelease(@Concurrency(2) ExecutorService executor) throws Exception {

        BlockingPollQueue backingQueue = new BlockingPollQueue();
        RequestQueue queue = new RequestQueue(new ConnectionContext(), backingQueue);
        TestExchange first = new TestExchange();
        TestExchange second = new TestExchange();
        TestExchange third = new TestExchange();

        queue.submit(first);
        backingQueue.blockNextEmptyPoll();

        Future<Boolean> release = executor.submit(() -> first.getLease().release());

        assertThat(backingQueue.awaitEmptyPoll()).isTrue();

        Future<?> submission = executor.submit(() -> queue.submit(second));

        assertThat(release.get(5, TimeUnit.SECONDS)).isTrue();
        submission.get(5, TimeUnit.SECONDS);

        queue.submit(third);

        assertThat(second.isAdmitted()).isTrue();
        assertThat(third.isAdmitted()).isFalse();
    }

    @Test
    void shouldAdmitEveryConcurrentSubmission(@Concurrency(32) ExecutorService executor) throws Exception {

        int concurrency = 32;

        for (int attempt = 0; attempt < 100; attempt++) {

            RequestQueue queue = new RequestQueue(new ConnectionContext());
            TestExchange first = new TestExchange();
            CountDownLatch ready = new CountDownLatch(concurrency);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger admitted = new AtomicInteger();
            List<Future<?>> submissions = new ArrayList<>();

            queue.submit(first);

            for (int i = 0; i < concurrency; i++) {
                submissions.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    queue.submit(new Sinkable() {

                        @Override
                        public void admit(RequestQueue.Lease lease) {
                            admitted.incrementAndGet();
                            lease.release();
                        }

                        @Override
                        public void fail(Throwable throwable) {
                            throw new AssertionError(throwable);
                        }
                    });
                    return null;
                }));
            }

            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            for (Future<?> submission : submissions) {
                submission.get(5, TimeUnit.SECONDS);
            }

            first.getLease().release();

            assertThat(admitted).hasValue(concurrency);
        }
    }

    @Test
    void shouldNotStrandTheQueueWhenAnAdmittedExchangeDeclinesTheWindow() {

        TestExchange third = new TestExchange();

        this.queue.submit(this.first);

        // an exchange whose subscriber left before admission hands the window straight on
        this.queue.submit(new TestExchange() {

            @Override
            public void admit(RequestQueue.Lease lease) {
                lease.release();
            }
        });
        this.queue.submit(third);

        this.first.getLease().release();

        assertThat(third.isAdmitted()).isTrue();
    }

    @Test
    void shouldFailQueuedExchangesOnTermination() {

        this.queue.submit(this.first);
        this.queue.submit(this.second);

        IllegalStateException failure = new IllegalStateException("Connection closed");

        assertThat(this.queue.terminate(failure)).isTrue();

        this.first.getLease().release();

        assertThat(this.second.isAdmitted()).isFalse();
        assertThat(this.second.getError()).isSameAs(failure);
    }

    @Test
    void shouldFailSubmissionAfterTermination() {

        this.queue.submit(this.first);

        IllegalStateException failure = new IllegalStateException("Connection closed");

        assertThat(this.queue.terminate(failure)).isTrue();

        this.queue.submit(this.second);

        assertThat(this.second.isAdmitted()).isFalse();
        assertThat(this.second.getError()).isSameAs(failure);
        assertThat(this.queue.terminate(new IllegalStateException("Other failure"))).isFalse();
    }

    static class TestExchange implements Sinkable {

        private volatile RequestQueue.Lease lease;

        private volatile Throwable error;

        @Override
        public void admit(RequestQueue.Lease lease) {
            this.lease = lease;
        }

        @Override
        public void fail(Throwable throwable) {
            this.error = throwable;
        }

        boolean isAdmitted() {
            return this.lease != null;
        }

        RequestQueue.Lease getLease() {
            return this.lease;
        }

        @Nullable
        Throwable getError() {
            return this.error;
        }

    }

    static class BlockingPollQueue extends ConcurrentLinkedQueue<Sinkable> {

        private final AtomicBoolean blockNextEmptyPoll = new AtomicBoolean();

        private final CountDownLatch emptyPoll = new CountDownLatch(1);

        private final CountDownLatch offerAfterEmptyPoll = new CountDownLatch(1);

        @Override
        public boolean offer(Sinkable sinkable) {

            boolean offered = super.offer(sinkable);

            if (this.emptyPoll.getCount() == 0) {
                this.offerAfterEmptyPoll.countDown();
            }

            return offered;
        }

        @Override
        @Nullable
        public Sinkable poll() {

            Sinkable sinkable = super.poll();

            if (sinkable == null && this.blockNextEmptyPoll.compareAndSet(true, false)) {
                this.emptyPoll.countDown();

                try {
                    this.offerAfterEmptyPoll.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }

            return sinkable;
        }

        void blockNextEmptyPoll() {
            this.blockNextEmptyPoll.set(true);
        }

        boolean awaitEmptyPoll() throws InterruptedException {
            return this.emptyPoll.await(5, TimeUnit.SECONDS);
        }

    }

}
