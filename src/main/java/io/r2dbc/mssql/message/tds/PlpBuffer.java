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

package io.r2dbc.mssql.message.tds;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.PlpLength;
import io.r2dbc.mssql.message.type.TypeInformation;

import java.util.function.Function;

/**
 * Utility to decode PLP (partially length-prefixed) streams. Instances encapsulate a {@link ByteBuf} and a {@link TypeInformation} and operate on a PLP stream in one of two representations:
 * <ul>
 * <li>The framed stream as it appears on the wire, consisting of the {@link PlpLength PLP length header}, chunk {@link Length length headers}, chunk data, and the zero-length terminator.
 * {@link #readRetainedStream()} extracts this representation from a token stream for later decoding.</li>
 * <li>The de-chunked payload without framing. {@link #decodeMap(Function)} and {@link #decodeByteArray()} produce this representation and expect the buffer positioned after the
 * PLP length header, i.e. after consuming it through {@link #decodeLength()} or {@link Length#decode(ByteBuf, TypeInformation)}.</li>
 * </ul>
 * Mainly for internal use within the driver.
 *
 * @author Mark Paluch
 * @since 1.0.6
 */
public final class PlpBuffer {

    private final ByteBuf buffer;

    private final TypeInformation type;

    private PlpBuffer(ByteBuf buffer, TypeInformation type) {
        this.buffer = buffer;
        this.type = type;
    }

    /**
     * Create a {@code PlpBuffer} for the given data buffer and type.
     *
     * @param buffer the data buffer positioned at a PLP stream. The buffer is read by the decode methods; it is not retained by this call.
     * @param type   the type descriptor providing the length strategy.
     * @return the {@code PlpBuffer} for {@code buffer} and {@code type}.
     */
    public static PlpBuffer of(ByteBuf buffer, TypeInformation type) {
        return new PlpBuffer(buffer, type);
    }

    /**
     * Returns whether the buffer contains a complete PLP stream, starting at the PLP length header and ending with the zero-length terminator.
     * The {@link ByteBuf#readerIndex()} is reset after the check.
     *
     * @return {@code true} if the buffer contains a complete PLP stream; {@code false} to await more data.
     */
    public boolean canDecode() {

        int readerIndex = this.buffer.readerIndex();
        try {
            if (!PlpLength.canDecode(this.buffer, this.type)) {
                return false;
            }

            PlpLength totalLength = decodeLength();

            if (totalLength.isNull()) {
                return true;
            }

            while (true) {

                if (!Length.canDecode(this.buffer, this.type)) {
                    return false;
                }

                Length chunkLength = Length.decode(this.buffer, this.type);

                if (chunkLength.isEmpty()) {
                    return true;
                }

                if (this.buffer.readableBytes() < chunkLength.getLength()) {
                    return false;
                }

                chunkLength.map(this.buffer::skipBytes);
            }
        } finally {
            this.buffer.readerIndex(readerIndex);
        }
    }

    /**
     * Decode the {@link PlpLength length} of the buffer, advancing the buffer past the PLP length header.
     *
     * @return the decoded {@link PlpLength}.
     */
    public PlpLength decodeLength() {
        return PlpLength.decode(this.buffer, this.type);
    }

    /**
     * Read the complete framed PLP stream (PLP length header, chunk length headers, chunk data, and terminator) as
     * retained slice, advancing the buffer past the stream. Use this method to extract a PLP value from a token
     * stream for later decoding.
     * <p>The buffer must be positioned at the PLP length header and contain the complete stream, see {@link #canDecode()}.
     * For a {@code NULL} value, the slice contains only the PLP length header.
     *
     * @return a retained slice of the framed PLP stream. The caller is responsible for releasing the returned buffer.
     */
    public ByteBuf readRetainedStream() {

        int startIndex = this.buffer.readerIndex();
        PlpLength totalLength = decodeLength();

        if (!totalLength.isNull()) {
            while (true) {

                Length chunkLength = Length.decode(this.buffer, this.type);

                if (chunkLength.isEmpty()) {
                    break;
                }

                chunkLength.map(this.buffer::skipBytes);
            }
        }

        int endIndex = this.buffer.readerIndex();
        this.buffer.readerIndex(startIndex);
        return this.buffer.readRetainedSlice(endIndex - startIndex);
    }

    /**
     * De-chunk the PLP payload and map it using {@code mapper}. The payload buffer is released after applying the mapping function.
     * <p>The buffer must be positioned after the PLP length header. Decoding stops at the zero-length terminator or when the buffer is exhausted.
     *
     * @param <T>    the result type.
     * @param mapper the mapping function receiving the de-chunked payload.
     * @return the mapped result.
     * @see #decodeByteArray()
     */
    public <T> T decodeMap(Function<ByteBuf, T> mapper) {

        ByteBuf decoded = aggregatePayload();
        try {
            return mapper.apply(decoded);
        } finally {
            decoded.release();
        }
    }

    /**
     * De-chunk the PLP payload into a byte array.
     * <p>The buffer must be positioned after the PLP length header. Decoding stops at the zero-length terminator
     * or when the buffer is exhausted.
     *
     * @return the de-chunked payload.
     * @see #decodeMap(Function)
     */
    public byte[] decodeByteArray() {
        return decodeMap(buffer -> {
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.readBytes(bytes);
            return bytes;
        });
    }

    /**
     * De-chunk the PLP payload into a composite of retained chunk slices, stopping at the zero-length terminator
     * or buffer exhaustion.
     */
    private ByteBuf aggregatePayload() {

        CompositeByteBuf result = this.buffer.alloc().compositeBuffer();

        while (this.buffer.isReadable()) {

            Length chunkLength = Length.decode(this.buffer, this.type);

            if (chunkLength.isEmpty()) {
                break;
            }

            result.addComponent(true, this.buffer.readRetainedSlice(chunkLength.getLength()));
        }
        return result;
    }

}
