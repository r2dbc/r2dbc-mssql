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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;
import reactor.core.Disposable;

import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Encoded value, either providing a singleton {@link ByteBuf} or a {@link Supplier} of buffers.
 *
 * @author Mark Paluch
 */
public class Encoded implements Disposable {

    private final TdsDataType dataType;

    private final Supplier<ByteBuf> encoder;

    Encoded(TdsDataType dataType, Supplier<ByteBuf> encoder) {
        this.dataType = dataType;
        this.encoder = encoder;
    }

    public static Encoded of(TdsDataType dataType, ByteBuf value) {
        return new Encoded(dataType, new DisposableSupplier(value));
    }

    public static Encoded of(TdsDataType dataType, Supplier<ByteBuf> value) {
        return new Encoded(dataType, value);
    }

    public TdsDataType getDataType() {
        return this.dataType;
    }

    public ByteBuf getValue() {
        return this.encoder.get();
    }

    /**
     * Returns the formal type such as {@literal INTEGER} or {@literal VARCHAR(255)}
     *
     * @return
     */
    public String getFormalType() {

        for (SqlServerType serverType : SqlServerType.values()) {

            for (TdsDataType tdsType : serverType.getFixedTypes()) {
                if (tdsType == this.dataType) {
                    return serverType.toString();
                }
            }
        }

        throw new IllegalStateException(String.format("Cannot determine a formal type for %s", this.dataType));
    }

    /**
     * Attempt to estimate the length of the buffer to apply allocation optimizations.
     *
     * @return the estimated length. Can be an approximation or zero, if the buffer size cannot be estimated.
     */
    public int estimateLength() {

        if (this.encoder instanceof DisposableSupplier) {
            return ((DisposableSupplier) this.encoder).get().readableBytes();
        }

        if (this.encoder instanceof LengthAwareSupplier) {
            return ((LengthAwareSupplier) this.encoder).getLength();
        }

        return 0;
    }

    public static Supplier<ByteBuf> ofLengthAware(int length, IntFunction<ByteBuf> supplier) {
        return new LengthAwareSupplier(length, supplier);
    }

    @Override
    public void dispose() {

        if (this.encoder instanceof DisposableSupplier) {
            ((DisposableSupplier) this.encoder).dispose();
        }
    }

    static class DisposableSupplier implements Supplier<ByteBuf>, Disposable {

        private final ByteBuf buf;

        DisposableSupplier(ByteBuf buf) {
            this.buf = buf;
        }

        @Override
        public ByteBuf get() {
            return buf.asReadOnly();
        }

        @Override
        public void dispose() {

            if (!isDisposed()) {
                buf.release();
            }
        }

        @Override
        public boolean isDisposed() {
            return buf.refCnt() == 0;
        }
    }


    static class LengthAwareSupplier implements Supplier<ByteBuf> {

        private final int length;

        private final IntFunction<ByteBuf> delegate;

        public LengthAwareSupplier(int length, IntFunction<ByteBuf> delegate) {
            this.length = length;
            this.delegate = delegate;
        }

        @Override
        public ByteBuf get() {
            return delegate.apply(length);
        }

        public int getLength() {
            return length;
        }
    }
}
