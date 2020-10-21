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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.codec.Encoded;
import io.r2dbc.mssql.codec.PlpEncoded;
import io.r2dbc.mssql.codec.RpcDirection;
import io.r2dbc.mssql.codec.RpcEncoding;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.TdsFragment;
import io.r2dbc.mssql.message.tds.TdsPackets;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.Assert;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RPC request to invoke stored procedures.
 *
 * @author Mark Paluch
 */
public final class RpcRequest implements ClientMessage, TokenStream {

    static final HeaderOptions HEADER = HeaderOptions.create(Type.RPC, Status.empty());

    /**
     * Requests positioned updates. This procedure performs operations on one or more rows within a cursor's fetch buffer.
     */
    public static final short Sp_Cursor = 1;

    /**
     * Opens a cursor. sp_cursoropen defines the SQL statement associated with the cursor and cursor options, and then populates the cursor.
     */
    public static final short Sp_CursorOpen = 2;

    /**
     * Compiles the cursor statement or batch into an execution plan, but does not create the cursor. The compiled statement can later be used by {@link #Sp_CursorExecute}.
     */
    public static final short Sp_CursorPrepare = 3;

    /**
     * Creates and populates a cursor based upon the execution plan created by {@link #Sp_CursorPrepare}. This procedure, coupled with {@link #Sp_CursorPrepare}, has the same function as
     * {@link #Sp_CursorOpen}, but is split into two phases.
     */
    public static final short Sp_CursorExecute = 4;

    /**
     * Compiles a plan for the submitted cursor statement or batch, then creates and populates the cursor. sp_cursorprepexec combines the functions of {@link #Sp_CursorPrepare} and
     * {@link #Sp_CursorExecute}.
     */
    public static final short Sp_CursorPrepExec = 5;

    /**
     * Discards the execution plan developed in the sp_cursorprepare stored procedure.
     */
    public static final short Sp_CursorUnprepare = 6;

    /**
     * Fetches a buffer of one or more rows from the database. The group of rows in this buffer is called the cursor's fetch buffer.
     */
    public static final short Sp_CursorFetch = 7;

    /**
     * Sets cursor options or returns cursor information created by the {@link #Sp_CursorOpen} stored procedure.
     */
    public static final short Sp_CursorOption = 8;

    /**
     * Closes and de-allocates the cursor, as well as releases all associated resources; that is, it drops the temporary table used in support of {@literal KEYSET} or {@literal STATIC} cursor.
     */
    public static final short Sp_CursorClose = 9;

    /**
     * Executes a Transact-SQL statement or batch that can be reused many times, or one that has been built dynamically.
     */
    public static final short Sp_ExecuteSql = 10;

    /**
     * Prepares a parameterized Transact-SQL statement and returns a statement handle for execution.
     */
    public static final short Sp_Prepare = 11;

    /**
     * Executes a prepared Transact-SQL statement using a specified handle and optional parameter value.
     */
    public static final short Sp_Execute = 12;

    /**
     * Prepares and executes a parameterized Transact-SQL statement. {@link #Sp_PrepExec} combines the functions of {@link #Sp_Prepare} and {@link #Sp_Execute}.
     */
    public static final short Sp_PrepExec = 13;

    /**
     * Prepares and executes a parameterized stored procedure call that has been specified using an RPC identifier.
     */
    public static final short Sp_PrepExecRpc = 14;

    /**
     * Discards the execution plan created by the sp_prepare stored procedure.
     */
    public static final short Sp_Unprepare = 15;

    private static final short PROC_ID_SWITCH = (short) 0xFFFF;

    private final AllHeaders allHeaders;

    @Nullable
    private final String procName;

    private final Integer procId;

    private final OptionFlags optionFlags;

    private final byte statusFlags;

    private final List<ParameterDescriptor> parameterDescriptors;

    private RpcRequest(AllHeaders allHeaders, @Nullable String procName, @Nullable Integer procId, OptionFlags optionFlags, byte statusFlags, List<ParameterDescriptor> parameterDescriptors) {

        this.allHeaders = Assert.requireNonNull(allHeaders, "AllHeaders must not be null");
        this.procName = procName;
        this.procId = procId;
        this.optionFlags = Assert.requireNonNull(optionFlags, "Option flags must not be null");
        this.statusFlags = statusFlags;
        this.parameterDescriptors = parameterDescriptors;
    }

    /**
     * Creates a new {@link Builder} to build a {@link RpcRequest}.
     *
     * @return a new {@link Builder} to build a {@link RpcRequest}.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Publisher<TdsFragment> encode(ByteBufAllocator allocator, int packetSize) {

        Assert.requireNonNull(allocator, "ByteBufAllocator must not be null");

        return Flux.defer(() -> {

            int name = 2 + (this.procName != null ? this.procName.length() * 2 : 0);
            int length = 4 + name + this.allHeaders.getLength();

            for (ParameterDescriptor descriptor : this.parameterDescriptors) {
                length += descriptor.estimateLength();
            }

            ByteBuf scalarBuffer = allocator.buffer(length);

            encodeHeader(scalarBuffer);

            boolean hasPlpSegments = false;

            for (ParameterDescriptor descriptor : this.parameterDescriptors) {
                if (descriptor instanceof EncodedRpcParameter) {
                    if (((EncodedRpcParameter) descriptor).getValue() instanceof PlpEncoded) {
                        hasPlpSegments = true;
                    }
                }
            }

            if (!hasPlpSegments) {

                for (ParameterDescriptor descriptor : this.parameterDescriptors) {
                    descriptor.encode(scalarBuffer);
                }

                return Flux.just(TdsPackets.create(HEADER, scalarBuffer));
            }

            AtomicReference<ByteBuf> firstBufferHolder = new AtomicReference<>(scalarBuffer);
            AtomicBoolean first = new AtomicBoolean(true);
            return Flux.fromIterable(this.parameterDescriptors).concatMap(it -> {

                ByteBuf buffer = getByteBuf(firstBufferHolder, allocator, it);

                if (it instanceof EncodedRpcParameter && ((EncodedRpcParameter) it).getValue() instanceof PlpEncoded) {

                    EncodedRpcParameter parameter = (EncodedRpcParameter) it;
                    PlpEncoded encoded = (PlpEncoded) parameter.getValue();

                    parameter.encodeHeader(buffer);
                    encoded.encodeHeader(buffer);

                    AtomicReference<ByteBuf> firstChunk = new AtomicReference<>(buffer);

                    Flux<ByteBuf> tdsFragments = encoded.chunked(() -> packetSize * 4, true).map(chunk -> {

                        if (firstChunk.compareAndSet(buffer, null)) {

                            CompositeByteBuf withInitialBuffer = allocator.compositeBuffer();

                            withInitialBuffer.addComponent(true, buffer);
                            withInitialBuffer.addComponent(true, chunk);

                            return withInitialBuffer;
                        }

                        return chunk;
                    });

                    return tdsFragments.concatWith(Mono.create(sink -> {

                        ByteBuf terminator = allocator.buffer();
                        Encode.asInt(terminator, 0);

                        sink.success(terminator);
                    }));
                }

                it.encode(buffer);
                return Mono.just(buffer);
            }, 1).map(buf -> {

                if (first.compareAndSet(true, false)) {
                    return TdsPackets.first(HEADER, buf);
                }

                return TdsPackets.create(buf);

            }).concatWith(Mono.create(sink -> {

                ByteBuf firstBuffer = firstBufferHolder.getAndSet(null);

                if (firstBuffer != null) {
                    sink.success(TdsPackets.last(firstBuffer));
                    return;
                }

                sink.success(TdsPackets.last(Unpooled.EMPTY_BUFFER));
            }));
        });
    }

    private ByteBuf getByteBuf(AtomicReference<ByteBuf> firstBufferHolder, ByteBufAllocator allocator, ParameterDescriptor it) {

        ByteBuf firstBuffer = firstBufferHolder.getAndSet(null);

        if (firstBuffer != null) {
            return firstBuffer;
        }

        int estimatedLength = it.estimateLength();
        return estimatedLength > 0 ? allocator.buffer(estimatedLength) : allocator.buffer();
    }

    private void encodeHeader(ByteBuf buffer) {

        this.allHeaders.encode(buffer);

        if (this.procId != null) {
            Encode.uShort(buffer, PROC_ID_SWITCH);
            Encode.uShort(buffer, this.procId);
        } else {
            Assert.state(this.procName != null, "ProcName must not be null if ProcId is not set.");
            Encode.unicodeStream(buffer, this.procName);
        }

        Encode.asByte(buffer, this.optionFlags.getValue());
        Encode.asByte(buffer, this.statusFlags);
    }

    @Nullable
    public String getProcName() {
        return this.procName;
    }

    public Integer getProcId() {
        return this.procId;
    }

    @Override
    public String getName() {
        return "RPCRequest";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RpcRequest)) {
            return false;
        }
        RpcRequest that = (RpcRequest) o;
        return this.statusFlags == that.statusFlags &&
            Objects.equals(this.allHeaders, that.allHeaders) &&
            Objects.equals(this.procName, that.procName) &&
            Objects.equals(this.procId, that.procId) &&
            Objects.equals(this.optionFlags, that.optionFlags) &&
            Objects.equals(this.parameterDescriptors, that.parameterDescriptors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.allHeaders, this.procName, this.procId, this.optionFlags, this.statusFlags, this.parameterDescriptors);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getName());
        sb.append(" [procName='").append(this.procName).append('\'');
        sb.append(", procId=").append(this.procId);
        sb.append(", optionFlags=").append(this.optionFlags);
        sb.append(", statusFlags=").append(this.statusFlags);
        sb.append(", parameterDescriptors=").append(this.parameterDescriptors);
        sb.append(']');
        return sb.toString();
    }

    /**
     * Builder for {@link RpcRequest}.
     */
    public final static class Builder {

        @Nullable
        private String procName;

        private Integer procId;

        private OptionFlags optionFlags = OptionFlags.empty();

        private byte statusFlags;

        private TransactionDescriptor transactionDescriptor;

        private List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();

        /**
         * Configure a procedure name.
         *
         * @param procName the name of the stored procedure to call.
         * @return {@code this} {@link Builder}.
         */
        public Builder withProcName(String procName) {

            Assert.requireNonNull(procName, "ProcName must not be null");

            this.procId = null;
            this.procName = procName;

            return this;
        }

        /**
         * Configure a procedureId to call a pre-defined stored procedure.
         *
         * @param id the stored procedure Id. See {@link RpcRequest#Sp_Cursor} and other {@literal Sp_} constants.
         * @return {@code this} {@link Builder}.
         */
        public Builder withProcId(int id) {

            this.procName = null;
            this.procId = id;

            return this;
        }

        /**
         * Add a {@link String} parameter to this RPC call.
         *
         * @param direction RPC parameter direction (in/out).
         * @param collation parameter encoding.
         * @param value     the parameter value, can be {@code null}.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link RpcDirection} or {@link Collation} is {@code null}.
         */
        public Builder withParameter(RpcDirection direction, Collation collation, @Nullable String value) {

            Assert.requireNonNull(direction, "RPC direction (in/out) must not be null");
            Assert.requireNonNull(collation, "Collation must not be null");

            this.parameterDescriptors.add(new RpcString(direction, null, collation, value));

            return this;
        }

        /**
         * Add a {@link Integer} parameter to this RPC call.
         *
         * @param direction RPC parameter direction (in/out).
         * @param value     the parameter value, can be {@code null}.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link RpcDirection} is {@code null}.
         */
        public Builder withParameter(RpcDirection direction, @Nullable Integer value) {

            Assert.requireNonNull(direction, "RPC direction (in/out) must not be null");

            this.parameterDescriptors.add(new RpcInt(direction, null, value));

            return this;
        }

        /**
         * Add an {@link Encoded} parameter to this RPC call.
         *
         * @param direction RPC parameter direction (in/out).
         * @param value     the parameter value.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link RpcDirection} or {@link Encoded} is {@code null}.
         */
        public Builder withParameter(RpcDirection direction, Encoded value) {

            Assert.requireNonNull(direction, "RPC direction (in/out) must not be null");
            Assert.requireNonNull(value, "Encoded parameter name must not be null");

            this.parameterDescriptors.add(new EncodedRpcParameter(direction, null, value));

            return this;
        }

        /**
         * Add a {@link String} parameter to this RPC call.
         *
         * @param direction RPC parameter direction (in/out).
         * @param name      the parameter name
         * @param collation parameter encoding.
         * @param value     the parameter value, can be {@code null}.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link RpcDirection}, {@code name} or {@link Collation} is {@code null}.
         */
        public Builder withNamedParameter(RpcDirection direction, String name, Collation collation, @Nullable String value) {

            Assert.requireNonNull(direction, "RPC direction (in/out) must not be null");
            Assert.requireNonNull(name, "Parameter name must not be null");
            Assert.requireNonNull(collation, "Collation must not be null");

            this.parameterDescriptors.add(new RpcString(direction, name, collation, value));

            return this;
        }

        /**
         * Add a {@link Integer} parameter to this RPC call.
         *
         * @param direction RPC parameter direction (in/out).
         * @param name      the parameter name
         * @param value     the parameter value, can be {@code null}.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link RpcDirection} or {@code name} is {@code null}.
         */
        public Builder withNamedParameter(RpcDirection direction, String name, @Nullable Integer value) {

            Assert.requireNonNull(direction, "RPC direction (in/out) must not be null");
            Assert.requireNonNull(name, "Parameter name must not be null");

            this.parameterDescriptors.add(new RpcInt(direction, name, value));

            return this;
        }

        /**
         * Add an {@link Encoded} parameter to this RPC call.
         *
         * @param direction RPC parameter direction (in/out).
         * @param name      the parameter name
         * @param value     the parameter value.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link RpcDirection}, {@code name}, or {@link Encoded} is {@code null}.
         */
        public Builder withNamedParameter(RpcDirection direction, String name, Encoded value) {

            Assert.requireNonNull(direction, "RPC direction (in/out) must not be null");
            Assert.requireNonNull(name, "Parameter name must not be null");
            Assert.requireNonNull(value, "Encoded parameter name must not be null");

            this.parameterDescriptors.add(new EncodedRpcParameter(direction, name, value));

            return this;
        }

        /**
         * Configure a {@link TransactionDescriptor}.
         *
         * @param transactionDescriptor the transaction descriptor.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link TransactionDescriptor} is {@code null}.
         */
        public Builder withTransactionDescriptor(TransactionDescriptor transactionDescriptor) {

            this.transactionDescriptor = Assert.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

            return this;
        }

        /**
         * Configure the {@link OptionFlags}.
         *
         * @param optionFlags the option flags to use.
         * @return {@code this} {@link Builder}.
         * @throws IllegalArgumentException when {@link OptionFlags} is {@code null}.
         */
        public Builder withOptionFlags(OptionFlags optionFlags) {

            this.optionFlags = Assert.requireNonNull(optionFlags, "OptionFlags must not be null");

            return this;
        }

        /**
         * Build a {@link RpcRequest}.
         *
         * @return a new {@link RpcRequest}.
         * @throws IllegalStateException when {@link TransactionDescriptor} or procedure name/id are not configured.
         */
        public RpcRequest build() {

            Assert.state(this.transactionDescriptor != null, "TransactionDescriptor is not configured");
            Assert.state(this.procName != null || this.procId != null, "Either procedure name or procedure Id required");

            return new RpcRequest(AllHeaders.transactional(this.transactionDescriptor.toBytes(), 1), this.procName, this.procId, this.optionFlags, this.statusFlags,
                new ArrayList<>(this.parameterDescriptors));
        }

    }

    /**
     * RPC option flags.
     */
    public final static class OptionFlags {

        private static final OptionFlags EMPTY = new OptionFlags(0x00);

        /**
         * Recompile the called procedure.
         */
        static final byte RPC_OPTION_RECOMPILE = (byte) 0x01;

        /**
         * The server sends No Meta Data only if fNoMetadata is set to 1 in the request (i.e. suppressing Column Metadata).
         */
        static final byte RPC_OPTION_NO_METADATA = (byte) 0x02;

        private final int optionByte;

        private OptionFlags(int optionByte) {
            this.optionByte = optionByte;
        }

        /**
         * Creates an empty {@link OptionFlags}.
         *
         * @return a new {@link OptionFlags}.
         */
        public static OptionFlags empty() {
            return EMPTY;
        }

        /**
         * Enable procedure recompilation.
         *
         * @return new {@link OptionFlags} with the option applied.
         */
        public OptionFlags enableRecompile() {
            return new OptionFlags(this.optionByte | RPC_OPTION_RECOMPILE);
        }

        /**
         * Disable metadata.
         *
         * @return new {@link OptionFlags} with the option applied.
         */
        public OptionFlags disableMetadata() {
            return new OptionFlags(this.optionByte | RPC_OPTION_NO_METADATA);
        }

        /**
         * @return the combined option byte.
         */
        public byte getValue() {
            return (byte) this.optionByte;
        }

    }

    /**
     * Abstract base class for RPC parameter implementations.
     */
    abstract static class ParameterDescriptor {

        private final RpcDirection direction;

        @Nullable
        private final String name;

        ParameterDescriptor(RpcDirection direction, @Nullable String name) {

            this.direction = Assert.requireNonNull(direction, "Direction must not be null");
            this.name = name;
        }

        /**
         * Encode the parameter value.
         *
         * @param buffer the data buffer to use as encode target.
         */
        abstract void encode(ByteBuf buffer);

        /**
         * Estimate the encoded parameter length.
         *
         * @return the estimated parameter length in bytes.
         */
        abstract int estimateLength();

        public RpcDirection getDirection() {
            return this.direction;
        }

        @Nullable
        public String getName() {
            return this.name;
        }

    }

    /**
     * String RPC parameter.
     */
    static class RpcString extends ParameterDescriptor {

        private final Collation collation;

        @Nullable
        private final String value;

        RpcString(RpcDirection direction, @Nullable String name, Collation collation, @Nullable String value) {
            super(direction, name);
            this.value = value;
            this.collation = collation;
        }

        @Override
        void encode(ByteBuf buffer) {
            RpcEncoding.encodeString(buffer, getName(), getDirection(), this.collation, this.value);
        }

        @Override
        int estimateLength() {
            return 16 + (this.value != null ? this.value.length() * 2 : 0);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RpcString)) {
                return false;
            }
            RpcString rpcString = (RpcString) o;
            return Objects.equals(this.collation, rpcString.collation) &&
                Objects.equals(this.value, rpcString.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.collation, this.value);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [name='").append(getName()).append('\'');
            sb.append(", value=").append(this.value);
            sb.append(']');
            return sb.toString();
        }

    }

    /**
     * Integer RPC parameter.
     */
    static class RpcInt extends ParameterDescriptor {

        @Nullable
        private final Integer value;

        RpcInt(RpcDirection direction, @Nullable String name, @Nullable Integer value) {
            super(direction, name);
            this.value = value;
        }

        @Override
        void encode(ByteBuf buffer) {
            RpcEncoding.encodeInteger(buffer, getName(), getDirection(), this.value);
        }

        @Override
        int estimateLength() {
            return this.value != null ? 5 : 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RpcInt)) {
                return false;
            }
            RpcInt rpcInt = (RpcInt) o;
            return Objects.equals(this.value, rpcInt.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.value);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [name='").append(getName()).append('\'');
            sb.append(", value=").append(this.value);
            sb.append(']');
            return sb.toString();
        }

    }

    /**
     * RPC parameter.
     */
    static class EncodedRpcParameter extends ParameterDescriptor {

        private final Encoded value;

        EncodedRpcParameter(RpcDirection direction, @Nullable String name, Encoded value) {
            super(direction, name);
            this.value = value;
        }

        public Encoded getValue() {
            return this.value;
        }

        @Override
        void encode(ByteBuf buffer) {
            encodeHeader(buffer);
            buffer.writeBytes(this.value.getValue());
            this.value.release();
        }

        void encodeHeader(ByteBuf buffer) {
            RpcEncoding.encodeHeader(buffer, getName(), getDirection(), this.value.getDataType());
        }

        @Override
        int estimateLength() {

            int estimate = 2 + (getName() != null ? (getName().length() + 1) * 2 : 0);
            estimate += this.value.getValue().readableBytes();

            return estimate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EncodedRpcParameter)) {
                return false;
            }
            EncodedRpcParameter encodedRpcParameter = (EncodedRpcParameter) o;
            return Objects.equals(this.value, encodedRpcParameter.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.value);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [name='").append(getName()).append('\'');
            sb.append(", value=").append(this.value);
            sb.append(']');
            return sb.toString();
        }

    }

}

