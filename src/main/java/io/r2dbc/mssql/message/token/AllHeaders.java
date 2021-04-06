/*
 * Copyright 2018-2021 the original author or authors.
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
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.util.Assert;

import java.util.Arrays;
import java.util.Objects;

/**
 * All Headers data structure.
 *
 * @author Mark Paluch
 */
public final class AllHeaders {

    private final byte[] transactionDescriptor;

    private final int outstandingRequestCount;

    private final int length;

    private AllHeaders(byte[] transactionDescriptor, int outstandingRequestCount) {

        this.transactionDescriptor = transactionDescriptor;
        this.outstandingRequestCount = outstandingRequestCount;

        int totalLength = 4; // DWORD

        totalLength += transactionDescriptor.length + /* outstanding request count */ 4 + 4 /* Length field DWORD */ + 2 /* type */;

        this.length = totalLength;
    }

    /**
     * Creates {@link AllHeaders} containing only a {@link TransactionDescriptor transactional} descriptor.
     *
     * @param transactionDescriptor the binary transaction descriptor.
     * @param outstandingRequests   number of outstanding requests
     * @return the {@link AllHeaders} for {@link TransactionDescriptor} and {@literal outstandingRequests}.
     * @throws IllegalArgumentException when {@link TransactionDescriptor} is {@code null}.
     */
    public static AllHeaders transactional(TransactionDescriptor transactionDescriptor, int outstandingRequests) {

        Assert.requireNonNull(transactionDescriptor, "Transaction descriptor must not be null");

        return transactional(transactionDescriptor.toBytes(), outstandingRequests);
    }

    /**
     * Creates {@link AllHeaders} containing only a {@link TransactionDescriptor transactional} descriptor.
     *
     * @param transactionDescriptor the binary transaction descriptor.
     * @param outstandingRequests   number of outstanding requests
     * @return the {@link AllHeaders} for {@literal transactionDescriptor} and {@literal outstandingRequests}.
     * @throws IllegalArgumentException when {@link TransactionDescriptor} is {@code null}.
     */
    public static AllHeaders transactional(byte[] transactionDescriptor, int outstandingRequests) {

        Assert.requireNonNull(transactionDescriptor, "Transaction descriptor must not be null");

        return new AllHeaders(transactionDescriptor, outstandingRequests);
    }

    /**
     * Encode the header.
     *
     * @param buffer the data buffer.
     */
    public void encode(ByteBuf buffer) {

        Encode.dword(buffer, this.length);
        Encode.dword(buffer, this.transactionDescriptor.length + /* outstanding request count */ 4 + 6);
        Encode.uShort(buffer, 0x02);
        buffer.writeBytes(this.transactionDescriptor);
        Encode.dword(buffer, this.outstandingRequestCount);
    }

    public int getLength() {
        return this.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AllHeaders)) {
            return false;
        }
        AllHeaders that = (AllHeaders) o;
        return this.outstandingRequestCount == that.outstandingRequestCount &&
            this.length == that.length &&
            Arrays.equals(this.transactionDescriptor, that.transactionDescriptor);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(this.outstandingRequestCount, this.length);
        result = 31 * result + Arrays.hashCode(this.transactionDescriptor);
        return result;
    }

}
