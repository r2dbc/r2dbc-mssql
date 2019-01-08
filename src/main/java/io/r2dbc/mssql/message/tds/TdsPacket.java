/*
 * Copyright 2018-2019 the original author or authors.
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

package io.r2dbc.mssql.message.tds;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.PacketIdProvider;
import io.r2dbc.mssql.util.Assert;

/**
 * Self-contained TDS packet containing a {@link Header} and {@link ByteBuf data}. Self-contained TDS packets must
 * consist of a single message that does not exceed the negotiated packet size.
 *
 * @author Mark Paluch
 */
public final class TdsPacket extends TdsFragment {

    private final Header header;

    TdsPacket(Header header, ByteBuf buffer) {

        super(buffer);

        this.header = Assert.requireNonNull(header, "Header must not be null!");

        int expectedBodySize = header.getLength() - Header.LENGTH;
        Assert.isTrue(buffer.readableBytes() == expectedBodySize,
            () -> String.format(
                "ByteBuffer body size does not match length field in header. Expected body size [%d], actual size [%d]",
                buffer.readableBytes(), expectedBodySize));
    }

    /**
     * Encode this packet using by obtaining a buffer from {@link ByteBufAllocator}.
     *
     * @param allocator the allocator.
     * @return the encoded buffer.
     * @throws IllegalArgumentException when {@link ByteBufAllocator} is {@code null}.
     */
    public ByteBuf encode(ByteBufAllocator allocator) {

        Assert.requireNonNull(allocator, "ByteBufAllocator must not be null");

        ByteBuf buffer = allocator.buffer(this.header.getLength());

        this.header.encode(buffer);
        buffer.writeBytes(getByteBuf());

        getByteBuf().release();

        return buffer;
    }

    /**
     * Encode this packet using by obtaining a buffer from {@link ByteBufAllocator} and calculate the packet Id from
     * {@link PacketIdProvider}.
     *
     * @param allocator        the allocator.
     * @param packetIdProvider the {@link PacketIdProvider}.
     * @return the encoded buffer.
     * @throws IllegalArgumentException when {@link ByteBufAllocator} or {@link PacketIdProvider} is {@code null}.
     */
    public ByteBuf encode(ByteBufAllocator allocator, PacketIdProvider packetIdProvider) {

        Assert.requireNonNull(allocator, "ByteBufAllocator must not be null");
        Assert.requireNonNull(packetIdProvider, "PacketIdProvider must not be null");

        ByteBuf buffer = allocator.buffer(this.header.getLength());

        this.header.encode(buffer, packetIdProvider);
        buffer.writeBytes(getByteBuf());

        getByteBuf().release();

        return buffer;
    }
}
