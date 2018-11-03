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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.codec.RpcDirection;
import io.r2dbc.mssql.codec.RpcParameters;
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
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * RPC request to invoke stored procedures.
 *
 * @author Mark Paluch
 */
public final class RpcRequest implements ClientMessage {

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

        this.allHeaders = Objects.requireNonNull(allHeaders, "AllHeaders must not be null");
        this.procName = procName;
        this.procId = procId;
        this.optionFlags = Objects.requireNonNull(optionFlags, "Option flags must not be null");
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
    public Publisher<TdsFragment> encode(ByteBufAllocator allocator) {

        return Mono.fromSupplier(() -> {

            int name = 2 + (this.procName != null ? this.procName.length() * 2 : 0);
            int length = 4 + name + this.allHeaders.getLength();

            for (ParameterDescriptor descriptor : parameterDescriptors) {
                length += descriptor.estimateLength();
            }

            ByteBuf buffer = allocator.buffer(length);

            encode(buffer);
            return TdsPackets.create(HEADER, buffer);
        });
    }

    private void encode(ByteBuf buffer) {

        this.allHeaders.encode(buffer);

        if (procId != null) {
            Encode.uShort(buffer, PROC_ID_SWITCH);
            Encode.uShort(buffer, this.procId);
        } else {
            Assert.state(this.procName != null, "ProcName must not be null if ProcId is not set.");
            Encode.unicodeStream(buffer, this.procName);
        }

        Encode.asByte(buffer, this.optionFlags.getValue());
        Encode.asByte(buffer, this.statusFlags);

        for (ParameterDescriptor descriptor : this.parameterDescriptors) {
            descriptor.encode(buffer);
        }
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
         * @return {@literal this} {@link Builder}.
         */
        public Builder withProcName(String procName) {

            Objects.requireNonNull(procName, "ProcName must not be null");

            this.procId = null;
            this.procName = procName;

            return this;
        }

        /**
         * Configure a procedureId to call a pre-defined stored procedure.
         *
         * @param id the stored procedure Id. See {@link RpcRequest#Sp_Cursor} and other {@literal Sp_} constants.
         * @return {@literal this} {@link Builder}.
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
         * @param value     the parameter value, can be {@literal null}.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withParameter(RpcDirection direction, Collation collation, @Nullable String value) {

            Objects.requireNonNull(direction, "RPC direction (in/out) must not be null");
            Objects.requireNonNull(collation, "Collation must not be null");

            parameterDescriptors.add(new RpcString(direction, null, collation, value));

            return this;
        }

        /**
         * Add a {@link Integer} parameter to this RPC call.
         *
         * @param direction RPC parameter direction (in/out).
         * @param value     the parameter value, can be {@literal null}.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withParameter(RpcDirection direction, @Nullable Integer value) {

            Objects.requireNonNull(direction, "RPC direction (in/out) must not be null");

            parameterDescriptors.add(new RpcInt(direction, null, value));

            return this;
        }

        /**
         * Add a {@link String} parameter to this RPC call.
         *
         * @param direction RPC parameter direction (in/out).
         * @param name      the parameter name
         * @param collation parameter encoding.
         * @param value     the parameter value, can be {@literal null}.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withNamedParameter(RpcDirection direction, String name, Collation collation, @Nullable String value) {

            Objects.requireNonNull(direction, "RPC direction (in/out) must not be null");
            Objects.requireNonNull(name, "Parameter name must not be null");
            Objects.requireNonNull(collation, "Collation must not be null");

            parameterDescriptors.add(new RpcString(direction, name, collation, value));

            return this;
        }

        /**
         * Add a {@link Integer} parameter to this RPC call.
         *
         * @param direction RPC parameter direction (in/out).
         * @param name      the parameter name
         * @param value     the parameter value, can be {@literal null}.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withNamedParameter(RpcDirection direction, @Nullable String name, @Nullable Integer value) {

            Objects.requireNonNull(direction, "RPC direction (in/out) must not be null");
            Objects.requireNonNull(name, "Parameter name must not be null");

            parameterDescriptors.add(new RpcInt(direction, name, value));

            return this;
        }

        /**
         * Configure a {@link TransactionDescriptor}.
         *
         * @param transactionDescriptor the transaction descriptor.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withTransactionDescriptor(TransactionDescriptor transactionDescriptor) {

            this.transactionDescriptor = Objects.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

            return this;
        }

        /**
         * Build a {@link RpcRequest}.
         *
         * @return a new {@link RpcRequest}.
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
            return new OptionFlags((byte) 0x00);
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

            this.direction = Objects.requireNonNull(direction, "Direction must not be null");
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
            return direction;
        }

        @Nullable
        public String getName() {
            return name;
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
            RpcParameters.encodeString(buffer, getDirection(), getName(), this.collation, this.value);
        }

        @Override
        int estimateLength() {
            return 16 + (this.value != null ? this.value.length() * 2 : 0);
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
            RpcParameters.encodeInteger(buffer, getDirection(), getName(), this.value);
        }

        @Override
        int estimateLength() {
            return this.value != null ? 5 : 0;
        }
    }
}

