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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.TdsFragment;
import io.r2dbc.mssql.message.tds.TdsPackets;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.mssql.util.ReferenceCountUtil;

import java.util.Objects;

/**
 * Attention signal to cancel a running operation.
 *
 * @author Mark Paluch
 * @author Tomasz Marciniak
 * @since 0.9
 */
public final class Attention implements ClientMessage, TokenStream {

    private static final HeaderOptions header = HeaderOptions.create(Type.ATTENTION, Status.empty());

    private final AllHeaders allHeaders;

    /**
     * Creates a new {@link Attention} token.
     *
     * @param outstandingRequests   the number of outstanding requests.
     * @param transactionDescriptor the transaction descriptor (8 byte).
     */
    private Attention(int outstandingRequests, byte[] transactionDescriptor) {

        Assert.requireNonNull(transactionDescriptor, "Transaction descriptor must not be null");

        this.allHeaders = AllHeaders.transactional(transactionDescriptor, outstandingRequests);
    }

    /**
     * Creates a new {@link Attention}.
     *
     * @param outstandingRequests   the number of outstanding requests.
     * @param transactionDescriptor the transaction descriptor
     * @return the {@link Attention} token.
     */
    public static Attention create(int outstandingRequests, TransactionDescriptor transactionDescriptor) {

        Assert.requireNonNull(transactionDescriptor, "Transaction descriptor must not be null");

        return new Attention(outstandingRequests, transactionDescriptor.toBytes());
    }

    @Override
    public TdsFragment encode(ByteBufAllocator allocator, int packetSize) {

        Assert.requireNonNull(allocator, "ByteBufAllocator must not be null");

        int length = this.allHeaders.getLength();

        ByteBuf buffer = allocator.buffer(length);
        encode(buffer);

        ReferenceCountUtil.maybeRelease(buffer);

        return TdsPackets.create(header, Unpooled.EMPTY_BUFFER);
    }

    void encode(ByteBuf buffer) {
        this.allHeaders.encode(buffer);
    }

    @Override
    public String getName() {
        return "ATTN";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Attention)) {
            return false;
        }
        Attention attn = (Attention) o;
        return Objects.equals(this.allHeaders, attn.allHeaders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.allHeaders);
    }

    @Override
    public String toString() {
        return getName();
    }

}
