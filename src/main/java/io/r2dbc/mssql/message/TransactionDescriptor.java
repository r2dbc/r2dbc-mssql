/*
 * Copyright 2018-2020 the original author or authors.
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

package io.r2dbc.mssql.message;

import io.netty.buffer.ByteBufUtil;
import io.r2dbc.mssql.util.Assert;

import java.util.Arrays;

/**
 * Descriptor for the transaction state.
 *
 * @author Mark Paluch
 */
public final class TransactionDescriptor {

    /**
     * Length in bytes of the binary transaction descriptor representation.
     */
    public static final int LENGTH = 8;

    private final byte[] descriptor;

    private TransactionDescriptor(byte[] descriptor) {

        Assert.requireNonNull(descriptor, "Descriptor bytes must not be null");
        Assert.isTrue(descriptor.length == LENGTH, "Descriptor must be 8 bytes long");

        this.descriptor = descriptor;
    }

    /**
     * Creates an empty {@link TransactionDescriptor}.
     *
     * @return the empty {@link TransactionDescriptor}.
     */
    public static TransactionDescriptor empty() {
        return new TransactionDescriptor(new byte[8]);
    }

    /**
     * Creates an {@link TransactionDescriptor} from {@code descriptor} bytes.
     *
     * @param descriptor descriptor bytes. Must be 8 bytes long.
     * @return the {@link TransactionDescriptor} for {@code descriptor} bytes.
     */
    public static TransactionDescriptor from(byte[] descriptor) {
        return new TransactionDescriptor(descriptor);
    }

    /**
     * @return the binary representation of this {@link TransactionDescriptor}.
     */
    public byte[] toBytes() {
        return this.descriptor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TransactionDescriptor)) {
            return false;
        }
        TransactionDescriptor that = (TransactionDescriptor) o;
        return Arrays.equals(this.descriptor, that.descriptor);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.descriptor);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [").append(ByteBufUtil.hexDump(this.descriptor));
        sb.append(']');
        return sb.toString();
    }

}
