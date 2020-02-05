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
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

/**
 * A notification of an environment change (for example, database, language, and so on).
 *
 * @author Mark Paluch
 * @author Lars Haatveit
 * @see EnvChangeType
 */
public final class EnvChangeToken extends AbstractDataToken {

    public static final byte TYPE = (byte) 0xE3;

    /**
     * The total length of the ENVCHANGE data stream (EnvValueData).
     */
    private final int length;

    /**
     * The type of environment change.
     */
    private final EnvChangeType changeType;

    private final byte[] newValue;

    private final byte[] oldValue;

    public EnvChangeToken(int length, EnvChangeType changeType, byte[] newValue, @Nullable byte[] oldValue) {

        super(TYPE);

        Assert.requireNonNull(changeType, "EnvChangeType must not be null");
        Assert.requireNonNull(newValue, "New value must not be null");

        this.length = length;
        this.changeType = changeType;
        this.newValue = newValue;
        this.oldValue = oldValue;
    }

    /**
     * Decode a {@link EnvChangeToken}.
     *
     * @param buffer the data buffer.
     * @return the decoded {@link EnvChangeToken}
     */
    public static EnvChangeToken decode(ByteBuf buffer) {

        int length = Decode.uShort(buffer);
        byte type = Decode.asByte(buffer);

        EnvChangeType envChangeType = EnvChangeType.valueOf(type);

        byte[] newValue;
        byte[] oldValue;

        // The routing message contains structured data, while the other environment change tokens contain old/new value pairs prefixed with data length.

        if (envChangeType == EnvChangeType.Routing) {

            newValue = new byte[length - 1];
            buffer.readBytes(newValue);

            oldValue = null;

        } else {

            int newValueLen = envChangeType.toByteLength(Decode.asByte(buffer));

            newValue = new byte[newValueLen];
            buffer.readBytes(newValue);

            int oldValueLen = envChangeType.toByteLength(Decode.asByte(buffer));
            oldValue = new byte[oldValueLen];
            buffer.readBytes(oldValue);
        }

        return new EnvChangeToken(length, envChangeType, newValue, oldValue);
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link EnvChangeType}.
     *
     * @param buffer the data buffer.
     * @return {@code true} if the buffer contains sufficient data to entirely decode a {@link EnvChangeType}.
     */
    public static boolean canDecode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        Integer requiredLength = Decode.peekUShort(buffer);

        return requiredLength != null && buffer.readableBytes() >= (requiredLength + /* length field */ 2);
    }

    @Override
    public String getName() {
        return "ENVCHANGE_TOKEN";
    }

    public int getLength() {
        return this.length;
    }

    public EnvChangeType getChangeType() {
        return this.changeType;
    }

    public byte[] getNewValue() {
        return this.newValue;
    }

    @Nullable
    public byte[] getOldValue() {
        return this.oldValue;
    }

    public String getOldValueString() {
        return new String(this.oldValue, 0, this.oldValue.length, ServerCharset.UNICODE.charset());
    }

    public String getNewValueString() {
        return new String(this.newValue, 0, this.newValue.length, ServerCharset.UNICODE.charset());
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getName());
        sb.append(" [length=").append(this.length);
        sb.append(", changeType=").append(this.changeType);
        sb.append(", newValue=").append(getNewValueString());
        sb.append(']');
        return sb.toString();
    }

    /**
     * Environment change payload type (type of environment change).
     */
    public enum EnvChangeType {

        Database(1) {
            @Override
            public int toByteLength(byte dataLength) {
                return super.toByteLength(dataLength) * 2;
            }
        },
        Language(2) {
            @Override
            public int toByteLength(byte dataLength) {
                return super.toByteLength(dataLength) * 2;
            }
        },
        Charset(3) {
            @Override
            public int toByteLength(byte dataLength) {
                return super.toByteLength(dataLength) * 2;
            }
        },
        Packetsize(4) {
            @Override
            public int toByteLength(byte dataLength) {
                return super.toByteLength(dataLength) * 2;
            }
        },
        UnicodeLCID(5) {
            @Override
            public int toByteLength(byte dataLength) {
                return super.toByteLength(dataLength) * 2;
            }
        },
        UnicodeSortingComparison(6) {
            @Override
            public int toByteLength(byte dataLength) {
                return super.toByteLength(dataLength) * 2;
            }
        },
        SQLCollation(7), BeginTx(8), CommitTx(9), RollbackTx(10), EnlistDTC(11), DefectTx(12), RealtimeLogShipping(13) {
            @Override
            public int toByteLength(byte dataLength) {
                return super.toByteLength(dataLength) * 2;
            }
        },
        PromoteTx(15), TXMgrAddress(16), TxEnd(17), RSETACK(18), UserInstance(19) {
            @Override
            public int toByteLength(byte dataLength) {
                return super.toByteLength(dataLength) * 2;
            }
        },
        // Routing is used for redirections to a different server
        Routing(20);

        private final byte type;

        EnvChangeType(int type) {
            this.type = (byte) type;
        }

        public byte getType() {
            return this.type;
        }

        /**
         * Lookup {@link EnvChangeType} by its by {@code value}.
         *
         * @param value the env change type byte value.
         * @return the resolved {@link EnvChangeType}.
         * @throws IllegalArgumentException if the {@code value} cannot be resolved to a {@link EnvChangeType}.
         */
        public static EnvChangeType valueOf(int value) {

            for (EnvChangeType envChangeType : values()) {
                if (envChangeType.getType() == (byte) value) {
                    return envChangeType;
                }
            }

            throw new IllegalArgumentException(String.format("Invalid env change type 0x%01X", value));
        }

        public int toByteLength(byte dataLength) {
            return dataLength;
        }
    }
}
