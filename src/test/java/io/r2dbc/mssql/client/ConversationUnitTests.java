/*
 * Copyright 2018 the original author or authors.
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

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.message.Message;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link Conversation}.
 *
 * @author Mark Paluch
 */
class ConversationUnitTests {

    private final Frame message = new Frame("MESSAGE");

    private final Frame done = new Frame("DONE");

    private final AtomicInteger terminations = new AtomicInteger();

    private final Conversation conversation = Conversation.until(this.done::equals);

    @Test
    void shouldRejectNullArguments() {

        assertThatIllegalArgumentException().isThrownBy(() -> Conversation.until(null));
        assertThatIllegalArgumentException().isThrownBy(() -> this.conversation.attach(null, () -> {
        }));
        assertThatIllegalArgumentException().isThrownBy(() -> this.conversation.attach(Flux.empty(), null));
        assertThatIllegalArgumentException().isThrownBy(() -> this.conversation.detach(null));
    }

    @Test
    void shouldEmitFinalMessageBeforeCompleting() {

        List<Object> signals = new ArrayList<>();

        attach(Flux.<Message>just(this.message, this.done))
                .doOnNext(signals::add)
                .doOnComplete(() -> signals.add("complete"))
                .as(StepVerifier::create)
                .expectNextCount(2)
                .expectComplete()
                .verify(Duration.ofSeconds(1));

        assertThat(signals).containsExactly(this.message, this.done, "complete");
    }

    @Test
    void shouldEvaluateCompletionBeforeDeliveringMessage() {

        // A message flow advances its protocol state while processing a message. That state describes the round trip
        // following the message and must not decide whether the message itself closed the window.
        AtomicBoolean delivered = new AtomicBoolean();
        Conversation conversation = Conversation.until(message -> delivered.get());

        conversation.attach(Flux.<Message>just(this.message, this.done).concatWith(Flux.never()), this.terminations::incrementAndGet)
                .doOnNext(message -> delivered.set(true))
                .as(StepVerifier::create)
                .expectNext(this.message)
                .expectNext(this.done)
                .expectComplete()
                .verify(Duration.ofSeconds(1));
    }

    @Test
    void shouldEndWindowOnceOnTheFinalMessage() {

        attach(Flux.<Message>just(this.message, this.done).concatWith(Flux.never()))
                .as(StepVerifier::create)
                .expectNextCount(2)
                .expectComplete()
                .verify(Duration.ofSeconds(1));

        assertThat(this.terminations).hasValue(1);
        assertThat(this.conversation.isTerminated()).isTrue();
        assertThat(this.conversation.isAbandoned()).isFalse();

        // the final message was delivered, not discarded
        assertThat(this.done.refCnt()).isOne();
    }

    @Test
    void shouldNotAbandonWindowWhenSubscriberCancelsOnFinalMessage() {

        // The shape of a message flow that completes itself on the final message: the cancellation arrives while that
        // message is being delivered, which is after the window has closed.
        attach(Flux.<Message>just(this.message, this.done).concatWith(Flux.never()))
                .takeUntil(this.done::equals)
                .as(StepVerifier::create)
                .expectNext(this.message, this.done)
                .expectComplete()
                .verify(Duration.ofSeconds(1));

        assertThat(this.terminations).hasValue(1);
        assertThat(this.conversation.isAbandoned()).isFalse();
    }

    @Test
    void shouldDrainCancelledConversationToFinalMessage() {

        Sinks.Many<Message> messages = Sinks.many().unicast().onBackpressureBuffer();

        attach(messages.asFlux()).transform(this.conversation::detach)
                .take(1)
                .as(StepVerifier::create)
                .then(() -> messages.tryEmitNext(this.message))
                .expectNext(this.message)
                .expectComplete()
                .verify(Duration.ofSeconds(1));

        // the subscriber is gone, the window keeps running
        assertThat(this.conversation.isCancelled()).isTrue();
        assertThat(this.conversation.isTerminated()).isFalse();
        assertThat(this.terminations).hasValue(0);

        assertThat(messages.tryEmitNext(this.done)).isEqualTo(Sinks.EmitResult.OK);

        assertThat(this.conversation.isTerminated()).isTrue();
        assertThat(this.conversation.isAbandoned()).isFalse();
        assertThat(this.terminations).hasValue(1);

        // the remainder was discarded on behalf of the subscriber that left
        assertThat(this.done.refCnt()).isZero();

        // message flows keep asking after the window ended, so cancellation must not be forgotten
        assertThat(this.conversation.isCancelled()).isTrue();
    }

    @Test
    void shouldAbandonWindowOnCancellationWithoutDetaching() {

        Sinks.Many<Message> messages = Sinks.many().unicast().onBackpressureBuffer();

        attach(messages.asFlux()).subscribe().dispose();

        // the remaining response cannot be consumed, so the connection cannot carry another conversation
        assertThat(this.terminations).hasValue(1);
        assertThat(this.conversation.isAbandoned()).isTrue();
    }

    @Test
    void shouldAbandonWindowWhenCompletionPredicateFails() {

        IllegalStateException error = new IllegalStateException("Completion predicate failed");
        Conversation conversation = Conversation.until(message -> {
            throw error;
        });

        Sinks.Many<Message> messages = Sinks.many().unicast().onBackpressureBuffer();

        conversation.attach(messages.asFlux(), this.terminations::incrementAndGet)
                .as(StepVerifier::create)
                .then(() -> messages.tryEmitNext(this.message))
                .expectErrorSatisfies(actual -> assertThat(actual).isSameAs(error))
                .verify(Duration.ofSeconds(1));

        assertThat(this.terminations).hasValue(1);

        // without a verdict the end of the window cannot be recognized, so it must not be handed on
        assertThat(conversation.isAbandoned()).isTrue();

        // the message reached no subscriber and is not leaked either
        assertThat(this.message.refCnt()).isZero();
    }

    @Test
    void shouldNotAbandonWindowWhenResponseStreamFails() {

        IllegalStateException error = new IllegalStateException("Connection closed");

        attach(Flux.error(error))
                .as(StepVerifier::create)
                .expectErrorSatisfies(actual -> assertThat(actual).isSameAs(error))
                .verify(Duration.ofSeconds(1));

        assertThat(this.terminations).hasValue(1);
        assertThat(this.conversation.isTerminated()).isTrue();

        // the response stream itself ended, so there is no remainder left to protect
        assertThat(this.conversation.isAbandoned()).isFalse();
    }

    @Test
    void shouldReleaseMessagesArrivingAfterTheWindowEnded() {

        AtomicReference<Subscriber<? super Message>> inbound = new AtomicReference<>();

        // a response stream that has not observed the cancellation yet
        Flux<Message> uncancellable = Flux.from(subscriber -> {

            inbound.set(subscriber);
            subscriber.onSubscribe(new Subscription() {

                @Override
                public void request(long n) {
                }

                @Override
                public void cancel() {
                }
            });
        });

        attach(uncancellable).subscribe().dispose();

        assertThat(this.conversation.isAbandoned()).isTrue();

        inbound.get().onNext(this.message);

        assertThat(this.message.refCnt()).isZero();
    }

    @Test
    void shouldClaimConversationOnlyOnce() {

        AtomicBoolean completed = new AtomicBoolean();
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        Sinks.Many<Message> messages = Sinks.many().unicast().onBackpressureBuffer();

        Flux<Message> exchange = attach(messages.asFlux());
        Disposable first = exchange.subscribe(message -> {
        }, firstError::set, () -> completed.set(true));

        exchange.as(StepVerifier::create)
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("Conversation can be subscribed only once"))
                .verify(Duration.ofSeconds(1));

        // the rejected subscription must not end the conversation that is running
        assertThat(this.terminations).hasValue(0);
        assertThat(this.conversation.isTerminated()).isFalse();

        assertThat(messages.tryEmitNext(this.done)).isEqualTo(Sinks.EmitResult.OK);

        assertThat(completed).isTrue();
        assertThat(firstError).hasNullValue();
        assertThat(this.terminations).hasValue(1);
        assertThat(this.conversation.isAbandoned()).isFalse();

        first.dispose();
    }

    private Flux<Message> attach(Flux<Message> response) {
        return this.conversation.attach(response, this.terminations::incrementAndGet);
    }

    /**
     * Reference-counted stand-in for a TDS token, so discarding a message can be observed.
     */
    private static final class Frame extends AbstractReferenceCounted implements Message {

        private final String name;

        private Frame(String name) {
            this.name = name;
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            return this;
        }

        @Override
        public String toString() {
            return this.name;
        }

        @Override
        protected void deallocate() {
        }

    }

}
