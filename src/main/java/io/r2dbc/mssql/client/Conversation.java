/*
 * Copyright 2018-2026 the original author or authors.
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

import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.mssql.util.Operators;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.SynchronousSink;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;

/**
 * A single request/response window on a TDS connection.
 * <p>TDS conversations are strictly serialized: a connection carries one conversation at a time and the conversation
 * must be consumed up to its final message before the connection accepts the next one. A {@link Conversation} owns that
 * window. It recognizes the final message, keeps the window running for a subscriber that has lost interest (see
 * {@link #detach(Flux)}), and records whether the window ended in a state that allows the connection to be reused.
 * <p>A window that ends before its final message has arrived is <em>abandoned</em>. Its remaining messages cannot be
 * told apart from the response of whatever conversation comes next, so an abandoned window must not be handed on; the
 * {@link Client} closes the connection instead. Subscribers that may cancel therefore apply {@link #detach(Flux)},
 * which consumes and discards the remainder rather than abandoning it.
 * <p>A {@link Conversation} is single-use. It is claimed when the {@link Flux} returned by
 * {@link Client#exchange(org.reactivestreams.Publisher, Conversation)} is subscribed; another subscription of the same
 * conversation is rejected with an {@link IllegalStateException}.
 *
 * @author Mark Paluch
 * @see Client#exchange(org.reactivestreams.Publisher, Conversation)
 * @since 1.0.6
 */
public final class Conversation {

    private static final AtomicReferenceFieldUpdater<Conversation, State> STATE = AtomicReferenceFieldUpdater.newUpdater(Conversation.class, State.class, "state");

    private final Predicate<Message> isDone;

    private volatile State state = State.FRESH;

    private volatile boolean cancelled;

    private Conversation(Predicate<Message> isDone) {
        this.isDone = isDone;
    }

    /**
     * Creates a single-use {@link Conversation} whose window ends with the first message matching {@code isDone}.
     * <p>{@code isDone} is evaluated in arrival order for every inbound message while the window is open, and
     * <em>before</em> the message is handed to the subscriber. Protocol state that the subscriber advances while
     * processing a message therefore does not influence whether that same message closed the window.
     *
     * @param isDone predicate determining the final message of the conversation.
     * @return a new {@link Conversation}.
     */
    public static Conversation until(Predicate<Message> isDone) {
        Assert.requireNonNull(isDone, "Completion predicate must not be null");
        return new Conversation(isDone);
    }

    /**
     * Detach the subscriber from {@code source} on cancellation while keeping the window running: the remaining
     * messages are consumed so the conversation reaches its final message, and discarded instead of being delivered.
     * <p>Cancelling anywhere else abandons the window, unless the final message has already closed it. Message flows
     * apply {@code detach} downstream of the stage that advances their protocol state, so that state keeps being
     * advanced while the remainder drains.
     *
     * @param source the message flow to detach from.
     * @return the detachable {@link Flux}.
     */
    public Flux<Message> detach(Flux<Message> source) {
        Assert.requireNonNull(source, "Source must not be null");
        return Operators.discardOnCancel(source, () -> this.cancelled = true).doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release);
    }

    /**
     * @return {@code true} once {@link #detach(Flux)} has observed a cancellation, that is, the subscriber left while
     * the window keeps draining. Stays {@code true} for the rest of the conversation, so message flows can wind down
     * the protocol instead of requesting more data.
     */
    public boolean isCancelled() {
        return this.cancelled;
    }

    /**
     * Consume {@code response} as this conversation: claim the conversation, end it with the first message matching the
     * completion predicate, and notify {@code onTermination} exactly once when the window is over.
     *
     * @param response      the inbound messages of this connection.
     * @param onTermination notified exactly once, for whichever terminal signal ends the window first.
     *                      {@link #isAbandoned()} tells it whether the window ended before its final message.
     * @return the messages of this conversation, ending with its final message, or an {@link IllegalStateException} if
     * this conversation was already claimed.
     */
    Flux<Message> attach(Flux<Message> response, Runnable onTermination) {

        Assert.requireNonNull(response, "Response must not be null");
        Assert.requireNonNull(onTermination, "Termination callback must not be null");

        return Flux.defer(() -> {

            if (!STATE.compareAndSet(this, State.FRESH, State.ACTIVE)) {
                return Flux.error(new IllegalStateException("Conversation can be subscribed only once"));
            }

            return response.handle(this::onResponse).doFinally(signal -> {

                if (signal == SignalType.ON_ERROR) {
                    // The response stream itself has ended, so there is no window left to protect.
                    STATE.compareAndSet(this, State.ACTIVE, State.FAILED);
                } else {
                    // Ending on anything but the final message leaves the remainder of the window unconsumed.
                    // onResponse closes the conversation while handling that message, which handle(…) delivers only
                    // afterwards, so a subscriber cancelling on it does not end up here.
                    STATE.compareAndSet(this, State.ACTIVE, State.ABANDONED);
                }

                onTermination.run();
            });
        });
    }

    /**
     * @return {@code true} if the window ended before its final message arrived, leaving a remainder that no
     * subsequent conversation can tell apart from its own response.
     */
    boolean isAbandoned() {
        return this.state == State.ABANDONED;
    }

    /**
     * @return {@code true} once the request/response window has ended.
     */
    boolean isTerminated() {
        State state = this.state;
        return state != State.FRESH && state != State.ACTIVE;
    }

    private void onResponse(Message message, SynchronousSink<Message> sink) {

        if (this.state != State.ACTIVE) {

            // The window has ended and this message belongs to no subscriber.
            ReferenceCountUtil.release(message);
            return;
        }

        boolean done;

        try {
            done = this.isDone.test(message);
        } catch (Throwable e) {

            // Without a verdict there is no way to tell where the window ends, so the remainder cannot be consumed.
            // Abandoning rather than completing keeps the connection from being handed to the next conversation.
            ReferenceCountUtil.release(message);
            STATE.compareAndSet(this, State.ACTIVE, State.ABANDONED);
            sink.error(e);
            return;
        }

        if (done) {
            // Close while still handling the message: handle(…) delivers it only after this method returns, so a
            // subscriber cancelling as it processes the final message finds the window closed rather than abandoned.
            STATE.compareAndSet(this, State.ACTIVE, State.CLOSED);
        }

        sink.next(message);

        if (done) {
            sink.complete();
        }
    }

    @Override
    public String toString() {
        return "Conversation[state=" + this.state + ", cancelled=" + this.cancelled + "]";
    }

    private enum State {

        /**
         * Not claimed by a subscription yet.
         */
        FRESH,

        /**
         * Claimed, window open.
         */
        ACTIVE,

        /**
         * Ended with its final message. The connection can carry the next conversation.
         */
        CLOSED,

        /**
         * Ended because the response stream did.
         */
        FAILED,

        /**
         * Ended before its final message arrived. The connection cannot be reused.
         */
        ABANDONED

    }

}
