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
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.codec.Decodable;
import io.r2dbc.mssql.codec.RpcDirection;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.type.Length;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

/**
 * A returned value from a RPC call.
 *
 * @author Mark Paluch
 * @see RpcDirection#OUT
 */
public class ReturnValue extends AbstractReferenceCounted implements DataToken {

    public static final byte TYPE = (byte) 0xAC;

    /**
     * Indicates the ordinal position of the output parameter in the original RPC call. Large Object output parameters are reordered to appear at the end of the stream. First the group of small
     * parameters is sent, followed by the group of large output parameters. There is no reordering within the groups.
     */
    private final int ordinal;

    /**
     * The parameter name.
     */
    @Nullable
    private final String parameterName;

    private final byte status;

    private final TypeInformation type;

    private final ByteBuf value;

    /**
     * Creates a new {@link ReturnValue}.
     *
     * @param ordinal       the ordinal position of the output parameter in the original RPC call.
     * @param parameterName the parameter name.
     * @param status        indicator whether the value is a {@literal OUT} value or a UDF.
     * @param type          type descriptor of this value.
     * @param value         the actual value.
     */
    public ReturnValue(int ordinal, @Nullable String parameterName, byte status, TypeInformation type, ByteBuf value) {

        super();

        this.ordinal = ordinal;
        this.parameterName = parameterName;
        this.status = status;
        this.type = type;
        this.value = value;
    }

    /**
     * Decode a {@link RowToken}.
     *
     * @param buffer              the data buffer.
     * @param encryptionSupported whether encryption is supported.
     * @return the {@link ReturnValue}.
     */
    public static ReturnValue decode(ByteBuf buffer, boolean encryptionSupported) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        int ordinal = Decode.uShort(buffer);
        String name = Decode.unicodeBString(buffer);
        byte status = Decode.asByte(buffer);
        TypeInformation type = TypeInformation.decode(buffer, true);

        // Preserve length for Codecs
        int beforeLengthDescriptor = buffer.readerIndex();
        Length length = Length.decode(buffer, type);

        int descriptorLength = buffer.readerIndex() - beforeLengthDescriptor;
        buffer.readerIndex(beforeLengthDescriptor);

        ByteBuf value = buffer.readRetainedSlice(descriptorLength + length.getLength());

        return new ReturnValue(ordinal, name, status, type, value);
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link ReturnValue}.
     *
     * @param buffer              the data buffer.
     * @param encryptionSupported whether encryption is supported.
     * @return {@code true} if the buffer contains sufficient data to entirely decode a {@link ReturnValue}.
     */
    public static boolean canDecode(ByteBuf buffer, boolean encryptionSupported) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        int readerIndex = buffer.readerIndex();

        try {

            int requiredLength = 3;
            if (buffer.readableBytes() >= requiredLength) {

                buffer.skipBytes(2);
                int nameLength = Decode.asByte(buffer);

                if (buffer.readableBytes() < (nameLength * 2) + /* status */ 1) {
                    return false;
                }

                buffer.skipBytes((nameLength * 2) + 1);

                if (!TypeInformation.canDecode(buffer, true)) {
                    return false;
                }

                TypeInformation type = TypeInformation.decode(buffer, true);

                if (!Length.canDecode(buffer, type)) {
                    return false;
                }

                Length length = Length.decode(buffer, type);

                if (buffer.readableBytes() >= length.getLength()) {
                    return true;
                }
            }
        } finally {
            buffer.readerIndex(readerIndex);
        }

        return false;
    }

    /**
     * Check whether the {@link Message} is a {@link ReturnValue} that matches the parameter {@literal name}.
     *
     * @param message the message.
     * @param name    the parameter name.
     * @return {@code true} if  the {@link Message} is a {@link ReturnValue} that matches the parameter {@literal name}.
     */
    public static boolean matches(Message message, String name) {

        Assert.requireNonNull(message, "Message must not be null");
        Assert.requireNonNull(name, "Name must not be null");

        return message instanceof ReturnValue && name.equals(((ReturnValue) message).getParameterName());
    }

    /**
     * Check whether the {@link Message} is a {@link ReturnValue} that matches the parameter {@literal ordinal}.
     *
     * @param message the message.
     * @param ordinal the parameter ordinal.
     * @return {@code true} if  the {@link Message} is a {@link ReturnValue} that matches the parameter {@literal ordinal}.
     */
    public static boolean matches(Message message, int ordinal) {

        Assert.requireNonNull(message, "Message must not be null");

        return message instanceof ReturnValue && ordinal == ((ReturnValue) message).getOrdinal();
    }

    public int getOrdinal() {
        return this.ordinal;
    }

    @Nullable
    public String getParameterName() {
        return this.parameterName;
    }

    public TypeInformation getValueType() {
        return this.type;
    }

    public byte getStatus() {
        return this.status;
    }

    public ByteBuf getValue() {
        return this.value;
    }

    /**
     * Create a {@link Decodable} from this {@link ReturnValue} to allow decoding.
     *
     * @return the {@link Decodable}.
     * @see Codecs#decode(ByteBuf, Decodable, Class)
     */
    public Decodable asDecodable() {

        return new Decodable() {

            @Override
            public TypeInformation getType() {
                return getValueType();
            }

            @Override
            public String getName() {
                return getParameterName() == null ? "" : getParameterName();
            }
        };
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String getName() {
        return "RETURNVALUE";
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        this.value.touch(hint);
        return this;
    }

    @Override
    protected void deallocate() {
        this.value.release();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [ordinal=").append(this.ordinal);
        sb.append(", parameterName='").append(this.parameterName).append('\'');
        sb.append(", value=").append(this.value);
        sb.append(", type=").append(this.type);
        sb.append(']');
        return sb.toString();
    }

}
