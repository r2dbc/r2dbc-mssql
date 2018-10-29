/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.Status;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Objects;
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
    public Flux<Message> decode(ByteBuf in, MessageDecoder decodeFunction) {
        Objects.requireNonNull(in, "in must not be null");

        return Flux.<List<Message>, DecoderState>generate(() -> {
            DecoderState decoderState = this.state.getAndSet(null);

            return decoderState == null ? DecoderState.initial(in) : decoderState.andChunk(in);
        }, (state, sink) -> {

            if (state.header == null) {
                if (!Header.canDecode(state.remainder)) {
                    this.state.set(state.retain());
                    sink.complete();
                    return state;
                }

                state = state.readHeader();
            }

            try {

                Header header = state.getRequiredHeader();

                if (state.canReadChunk()) {
                    state = state.readChunk();
                } else {
                    this.state.set(state.retain());
                    sink.complete();
                    return state;
                }

                int readerIndex = state.unchunkedBodyData.readerIndex();

                List<Message> messages = (List) decodeFunction.apply(header, state.unchunkedBodyData);

                if (!messages.isEmpty()) {
                    sink.next(messages);

                    if (state.remainder.readableBytes() != 0) {
                        return state;
                    }

                    if (state.unchunkedBodyData.readableBytes() != 0) {
                        this.state.set(state.retain());
                    }
                } else {
                    state.unchunkedBodyData.readerIndex(readerIndex);
                    this.state.set(state.retain());
                }

                sink.complete();

                return state;
            } catch (Exception e) {
                sink.error(e);
            }

            return state;
        }, DecoderState::release).flatMapIterable(Function.identity());
    }

    @Nullable
    DecoderState getDecoderState() {
        return this.state.get();
    }

    /**
     * The current decoding state.
     */
    static class DecoderState {

        final ByteBuf remainder;

        final ByteBuf unchunkedBodyData;

        @Nullable
        final Header header;

        private DecoderState(ByteBuf remainder, ByteBuf unchunkedBodyData, @Nullable Header header) {
            this.remainder = remainder;
            this.unchunkedBodyData = unchunkedBodyData;
            this.header = header;
        }

        private DecoderState(ByteBuf initialBuffer, ByteBuf unchunkedBodyData) {
            this.remainder = initialBuffer;
            this.unchunkedBodyData = unchunkedBodyData;
            this.header = null;
        }

        public DecoderState withHeader(Header header) {
            return new DecoderState(this.remainder, this.unchunkedBodyData, header);
        }

        /**
         * Create a new, initial {@link DecoderState}.
         *
         * @param initialBuffer the data buffer.
         * @return the initial {@link DecoderState}.
         */
        static DecoderState initial(ByteBuf initialBuffer) {
            return new DecoderState(initialBuffer, Unpooled.EMPTY_BUFFER);
        }

        boolean canReadChunk() {

            int requiredChunkLength = getChunkLength();

            return this.remainder.readableBytes() >= requiredChunkLength;
        }

        private int getChunkLength() {
            return getRequiredHeader().getLength() - Header.LENGTH;
        }

        /**
         * Create a new {@link DecoderState} with a decoded {@link Header}.
         *
         * @return the new {@link DecoderState}.
         */
        DecoderState readHeader() {
            return new DecoderState(this.remainder, this.unchunkedBodyData, Header.decode(this.remainder));
        }

        public Header getRequiredHeader() {

            if (this.header == null) {
                throw new IllegalStateException("Header not decoded");
            }

            return this.header;
        }

        /**
         * Read the body chunk and create a new {@link DecoderState}.
         * Body is read from the remainder by copying the contents to decouple the remainder from dechunked data. Otherwise we would probably overwrite remainder data with dechunking.
         *
         * @return the new {@link DecoderState}.
         */
        DecoderState readChunk() {

            if (unchunkedBodyData == Unpooled.EMPTY_BUFFER) {

                ByteBuf unchunkedBodyData = this.remainder.copy(this.remainder.readerIndex(), getChunkLength());
                this.remainder.skipBytes(getChunkLength());
                return new DecoderState(remainder, unchunkedBodyData, null);
            }

            ByteBuf unchunkedBodyData = this.unchunkedBodyData.writeBytes(this.remainder.readSlice(getChunkLength()));
            return new DecoderState(this.remainder, unchunkedBodyData, getRequiredHeader());
        }

        DecoderState andChunk(ByteBuf in) {
            return new DecoderState(Unpooled.wrappedBuffer(this.remainder, in), this.unchunkedBodyData, this.header);
        }

        /**
         * Retain this {@link DecoderState} (i.e. increment ref count).
         *
         * @return {@code this} {@link DecoderState}.
         */
        DecoderState retain() {
            this.remainder.retain();
            this.unchunkedBodyData.retain();
            return this;
        }

        /**
         * Release this {@link DecoderState} (i.e. decrement ref count).
         */
        void release() {
            this.remainder.release();
            this.unchunkedBodyData.release();
        }
    }
}
