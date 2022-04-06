/*
 * Copyright 2018-2022 the original author or authors.
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
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.ArrayList;
import java.util.List;

/**
 * A TDS decoder that reads {@link ByteBuf}s and returns a {@link Flux} of decoded {@link Message}s.
 * <p/>
 * TDS messages consist of a header ({@link Header#LENGTH 8 byte length}) and a body. Messages can be either self-contained ({@link Status.StatusBit#EOM}) or chunked.  This decoder attempts to
 * decode messages from a {@link ByteBuf stream} by emitting zero, one or many {@link Message}s. Data buffers are aggregated and de-chunked until reaching a message boundary, then adaptive decoding
 * attempts to decode the aggregated and de-chunked body as far as possible. Remaining (non-decodable) data buffers are aggregated until the next attempt.
 * <p/>
 * This decoder is stateful and should be used in a try-to-decode fashion.
 *
 * @author Mark Paluch
 * @see Message
 * @see Header
 */
final class StreamDecoder {

    private DecoderState state;

    /**
     * Decode a {@link ByteBuf} into a {@link Flux} of {@link Message}s. If the {@link ByteBuf} does not end on a
     * {@link Message} boundary, the {@link ByteBuf} will be retained until the concatenated contents of all retained
     * {@link ByteBuf}s is a {@link Message} boundary.
     *
     * @param in the {@link ByteBuf} to decode
     * @return a {@link Flux} of {@link Message}s
     */
    public List<Message> decode(ByteBuf in, MessageDecoder messageDecoder) {

        Assert.requireNonNull(in, "in must not be null");
        Assert.requireNonNull(messageDecoder, "MessageDecoder must not be null");

        ListSink<Message> result = new ListSink<>();

        decode(in, messageDecoder, result);

        return result;
    }

    /**
     * Decode a {@link ByteBuf} into a stream of {@link Message}s notifying {@link SynchronousSink}. If the {@link ByteBuf} does not end on a
     * {@link Message} boundary, the {@link ByteBuf} will be retained until the concatenated contents of all retained
     * {@link ByteBuf}s is a {@link Message} boundary.
     *
     * @param in the {@link ByteBuf} to decode
     * @return a {@link Flux} of {@link Message}s
     */
    public void decode(ByteBuf in, MessageDecoder messageDecoder, SynchronousSink<Message> sink) {

        Assert.requireNonNull(in, "in must not be null");
        Assert.requireNonNull(messageDecoder, "MessageDecoder must not be null");

        DecoderState decoderState = this.state;
        this.state = null;

        DecoderState state = decoderState == null ? DecoderState.initial(in) : decoderState.andChunk(in);

        do {
            state = withState(messageDecoder, sink, state);
        } while (state != null);
    }

    @Nullable
    private DecoderState withState(MessageDecoder messageDecoder, SynchronousSink<Message> sink, DecoderState state) {

        if (state.header == null) {

            if (!Header.canDecode(state.remainder)) {
                return retain(state);
            }

            state = state.readHeader();
        }

        try {

            Header header = state.getRequiredHeader();

            if (!state.canReadChunk()) {
                return retain(state);
            }

            state = state.readChunk();

            int readerIndex = state.aggregatedBodyReaderIndex();

            boolean hasMessages = messageDecoder.decode(header, state.aggregatedBody, sink);

            if (hasMessages) {

                if (state.hasRawRemainder()) {
                    return state;
                }

                if (state.hasAggregatedBodyRemainder()) {
                    return retain(state);
                }
            } else {
                state.aggregatedBodyReaderIndex(readerIndex);
                return retain(state);
            }

            state.release();
            return null;
        } catch (Exception e) {
            sink.error(e);
        }

        return state;
    }

    @Nullable
    private DecoderState retain(DecoderState state) {
        this.state = state;
        return null;
    }

    @Nullable
    DecoderState getDecoderState() {
        return this.state;
    }

    /**
     * The current decoding state. Encapsulates the raw transport stream buffers ("remainder") and the aggregated (de-chunked) body along an optional {@link Header}.
     */
    static class DecoderState {

        ByteBuf remainder;

        ByteBuf aggregatedBody;

        @Nullable
        Header header;

        private DecoderState(ByteBuf remainder, ByteBuf aggregatedBody, @Nullable Header header) {

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

            ByteBuf composite = initialBuffer.alloc().buffer();
            composite.writeBytes(initialBuffer);

            ByteBuf aggregatedBody = initialBuffer.alloc().buffer();

            return new DecoderState(composite, aggregatedBody, null);
        }

        /**
         * Create a new {@link DecoderState} by appending a new raw remaining {@link ByteBuf data buffer}.
         *
         * @param in
         * @return
         */
        DecoderState andChunk(ByteBuf in) {

            this.remainder.writeBytes(in);
            //this.remainder.addComponent(true, in.retain());
            return newState(this.remainder, this.aggregatedBody, this.header);
        }

        DecoderState newState(ByteBuf remainder, ByteBuf aggregatedBody, @Nullable Header header) {

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
         * Retains a new header if we were able to decode the header but not the rest of the chunk. Not retaining the header causes the header to be decoded on the next pass and that causes
         * protocol out of sync.
         * Drop the header if we were able to dechunk the remainder.
         *
         * @return the new {@link DecoderState}.
         */
        DecoderState readChunk() {

            boolean hasNewHeader;

            do {
                hasNewHeader = false;
                this.aggregatedBody.writeBytes(this.remainder, getChunkLength());

                if (Header.canDecode(this.remainder)) {
                    hasNewHeader = true;
                    this.header = Header.decode(this.remainder);
                }

            } while (canReadChunk());

            if (hasNewHeader) {
                return newState(this.remainder, this.aggregatedBody, this.header);
            }

            return newState(this.remainder, this.aggregatedBody, null);
        }

        /**
         * Retain this {@link DecoderState} (i.e. increment ref count).
         *
         * @return {@code this} {@link DecoderState}.
         */
        DecoderState retain() {

            //this.remainder.consolidate();
            this.remainder.retain();

            //this.aggregatedBody.consolidate();
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

    static class ListSink<T> extends ArrayList<T> implements SynchronousSink<T> {

        public ListSink() {
            super(2);
        }

        @Override
        public void complete() {
            throw new UnsupportedOperationException();
        }

        @Deprecated
        @Override
        public Context currentContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ContextView contextView() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void error(Throwable e) {
            throw new RuntimeException(e);
        }

        @Override
        public void next(T message) {
            add(message);
        }

    }

}
