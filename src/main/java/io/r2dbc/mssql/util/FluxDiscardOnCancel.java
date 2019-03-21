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

package io.r2dbc.mssql.util;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A decorating operator that replays signals from its source to a {@link Subscriber} and drains the source upon {@link Subscription#cancel() cancel} and drops data signals until termination.
 * Draining data is required to complete a particular request/response window and clear the protocol state as client code expects to start a request/response conversation without any previous
 * response state.
 *
 * @author Mark Paluch
 */
class FluxDiscardOnCancel<T> extends FluxOperator<T, T> {

    FluxDiscardOnCancel(Flux<? extends T> source) {
        super(source);
        onAssembly(this);
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        source.subscribe(new FluxDiscardOnCancelSubscriber<>(actual));
    }

    static class FluxDiscardOnCancelSubscriber<T> extends AtomicBoolean implements CoreSubscriber<T>, Subscription {

        final CoreSubscriber<T> actual;

        final Context ctx;

        Subscription s;

        volatile boolean cancelled;

        FluxDiscardOnCancelSubscriber(CoreSubscriber<T> actual) {

            this.actual = actual;
            this.ctx = actual.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {

            if (Operators.validate(this.s, s)) {
                this.s = s;
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {

            if (cancelled) {
                Operators.onDiscard(t, this.ctx);
                return;
            }

            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            s.request(n);
        }

        @Override
        public void cancel() {

            if (compareAndSet(false, true)) {
                this.cancelled = true;
                s.request(Long.MAX_VALUE);
            }
        }
    }
}
