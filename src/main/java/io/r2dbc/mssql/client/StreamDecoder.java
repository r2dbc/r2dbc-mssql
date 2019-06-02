/*
 * Copyright 2018-2019 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * A TDS decoder that reads {@link ByteBuf}s and returns a {@link Flux} of decoded {@link Message}s.
 * <p/>
 * TDS messages consist of a header ({@link Header#LENGTH 8 byte length}) and a body. Messages can be either self-contained ({@link Status.StatusBit#EOM}) or chunked.  This decoder attempts to
 * decode messages from a {@link ByteBuf stream} by emitting zero, one or many {@link Message}s. Data buffers are aggregated and de-chunked until reaching a message boundary, then adaptive decoding
 * attempts to decode the aggregated and de-chunked body as far as possible. Remaining (undecoded) data buffers are aggregated until the next attempt.
 * <p/>
 * This decoder is stateful and should be used in a try-to-decode fashion.
 *
 * @author Mark Paluch
 * @see Message
 * @see Header
 */
final class StreamDecoder {

    private final AtomicReference<DecoderState> state = new AtomicReference<>();

    /**
     * Decode a {@link ByteBuf} into a {@link Flux} of {@link Message}s. If the {@link ByteBuf} does not end on a
     * {@link Message} boundary, the {@link ByteBuf} will be retained until the concatenated contents of all retained
     * {@link ByteBuf}s is a {@link Message} boundary.
     *
     * @param in the {@link ByteBuf} to decode
     * @return a {@link Flux} of {@link Message}s
     */
    @SuppressWarnings("unchecked")
    public Flux<Message> decode(ByteBuf in, MessageDecoder messageDecoder) {

        Assert.requireNonNull(in, "in must not be null");
        Assert.requireNonNull(messageDecoder, "MessageDecoder must not be null");

        return Flux.<List<Message>, DecoderState>generate(() -> {
            DecoderState decoderState = this.state.getAndSet(null);

            return decoderState == null ? DecoderState.initial(in) : decoderState.andChunk(in);
        }, (state, sink) -> {

            if (state.header == null) {

                if (!Header.canDecode(state.remainder)) {
                    return retain(state, sink);
                }

                state = state.readHeader();
            }

            try {

                Header header = state.getRequiredHeader();

                if (!state.canReadChunk()) {
                    return retain(state, sink);
                }

                state = state.readChunk();

                int readerIndex = state.aggregatedBodyReaderIndex();

                List<Message> messages = (List) messageDecoder.apply(header, state.aggregatedBody);

                if (!messages.isEmpty()) {
                    sink.next(messages);

                    if (state.hasRawRemainder()) {
                        return state;
                    }

                    if (state.hasAggregatedBodyRemainder()) {
                        return retain(state, sink);
                    }
                } else {
                    state.aggregatedBodyReaderIndex(readerIndex);
                    return retain(state, sink);
                }

                sink.complete();
                return state;
            } catch (Exception e) {
                sink.error(e);
            }

            return state;
        }, state -> {
            if (state != null) {
                state.release();
            }
        }).flatMapIterable(Function.identity());
    }

    DecoderState retain(DecoderState state, SynchronousSink<?> sink) {
        this.state.set(state.retain());
        sink.complete();
        return state;
    }

    @Nullable
    DecoderState getDecoderState() {
        return this.state.get();
    }

    /**
     * The current decoding state. Encapsulates the raw transport stream buffers ("remainder") and the aggregated (de-chunked) body along an optional {@link Header}.
     */
    static class DecoderState {

        CompositeByteBuf remainder;

        CompositeByteBuf aggregatedBody;

        @Nullable
        Header header;

        private DecoderState(CompositeByteBuf remainder, CompositeByteBuf aggregatedBody, @Nullable Header header) {

            this.remainder = remainder;
            this.header = header;
            this.aggregatedBody = aggregatedBody;
        }

        /**
         * Create a new, initial {@link DecoderState}.
         *
         * @param initialBuffer the data buffer.
         * @return the initial {@link DecoderState}.
         */
        static DecoderState initial(ByteBuf initialBuffer) {

            CompositeByteBuf composite = initialBuffer.alloc().compositeBuffer();
            composite.addComponent(true, initialBuffer.retain());

            CompositeByteBuf aggregatedBody = initialBuffer.alloc().compositeBuffer();

            return new DecoderState(composite, aggregatedBody, null);
        }

        /**
         * Create a new {@link DecoderState} by appending a new raw remaining {@link ByteBuf data buffer}.
         *
         * @param in
         * @return
         */
        DecoderState andChunk(ByteBuf in) {
            this.remainder.addComponent(true, in.retain());
            return newState(this.remainder, this.aggregatedBody, this.header);
        }

        DecoderState newState(CompositeByteBuf remainder, CompositeByteBuf aggregatedBody, @Nullable Header header) {

            this.remainder = remainder;
            this.aggregatedBody = aggregatedBody;
            this.header = header;

            return this;
        }

        boolean canReadChunk() {

            int requiredChunkLength = getChunkLength();
            return this.remainder.readableBytes() >= requiredChunkLength;
        }

        /**
         * @return {@code true} if the remaining raw bytes (raw transport buffer) are not yet fully consumed.
         */
        boolean hasRawRemainder() {
            return this.remainder.readableBytes() != 0;
        }

        /**
         * @return {@code true} if the remaining aggregated body bytes (aggregation of body buffers without header) are not yet fully consumed.
         */
        boolean hasAggregatedBodyRemainder() {
            return this.aggregatedBody.readableBytes() != 0;
        }

        /**
         * @return the aggregated body reader index.
         */
        int aggregatedBodyReaderIndex() {
            return this.aggregatedBody.readerIndex();
        }

        /**
         * Reset the aggregated body reader index.
         *
         * @param index the reader index.
         */
        void aggregatedBodyReaderIndex(int index) {
            this.aggregatedBody.readerIndex(index);
        }

        /**
         * @return the required {@link Header}.
         */
        Header getRequiredHeader() {

            if (this.header == null) {
                throw new IllegalStateException("DecoderState has no header");
            }

            return this.header;
        }

        // ----------------------------------------
        // State-changing methods.
        // ----------------------------------------

        /**
         * Create a new {@link DecoderState} with a decoded {@link Header}.
         *
         * @return the new {@link DecoderState}.
         */
        DecoderState readHeader() {
            return newState(this.remainder, this.aggregatedBody, Header.decode(this.remainder));
        }

        /**
         * Read the body chunk and create a new {@link DecoderState}.
         * Body is read from the remainder by copying the contents to decouple the remainder from dechunked data. Otherwise we would probably overwrite remainder data with dechunking.
         *
         * @return the new {@link DecoderState}.
         */
        DecoderState readChunk() {

            do {

                this.aggregatedBody.addComponent(true, this.remainder.readRetainedSlice(getChunkLength()));

                if (!this.header.is(Status.StatusBit.EOM) && Header.canDecode(this.remainder)) {
                    this.header = Header.decode(this.remainder);
                }

            } while (canReadChunk());

            return newState(this.remainder, this.aggregatedBody, null);
        }

        /**
         * Retain this {@link DecoderState} (i.e. increment ref count).
         *
         * @return {@code this} {@link DecoderState}.
         */
        DecoderState retain() {

            this.remainder.consolidate();
            this.remainder.retain();

            this.aggregatedBody.consolidate();
            this.aggregatedBody.retain();

            return this;
        }

        /**
         * Release this {@link DecoderState} (i.e. decrement ref count).
         */
        void release() {

            this.remainder.release();
            this.aggregatedBody.release();
        }

        int getChunkLength() {
            return getRequiredHeader().getLength() - Header.LENGTH;
        }
    }
}
