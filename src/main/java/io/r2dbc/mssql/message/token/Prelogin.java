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
import io.r2dbc.mssql.client.ProtocolException;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.Header;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Status.StatusBit;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.ContextualTdsFragment;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.TdsFragment;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Stream structure for {@code PRELOGIN}.
 *
 * @author Mark Paluch
 * @see Token
 */
public final class Prelogin implements TokenStream, ClientMessage {

    private static final HeaderOptions HEADER_OPTIONS = HeaderOptions.create(Type.PRE_LOGIN, Status.of(StatusBit.EOM));

    private final int size;

    private final List<? extends Token> tokens;

    /**
     * Create a new {@link Prelogin} given {@link List} of {@link Token}.
     *
     * @param tokens must not be null.
     */
    public Prelogin(List<? extends Token> tokens) {

        Objects.requireNonNull(tokens, "Tokens must not be null");

        this.size = getSize(tokens);
        this.tokens = tokens;
    }

    /**
     * @return a new builder for {@link Prelogin}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Decode the {@link Prelogin} response from a {@link ByteBuf}.
     *
     * @param buffer must not be null.
     * @return the decoded {@link Prelogin} response {@link Message}.
     */
    public static Prelogin decode(ByteBuf buffer) {

        Objects.requireNonNull(buffer, "ByteBuf must not be null");

        List<Token> decodedTokens = new ArrayList<>();
        Prelogin prelogin = new Prelogin(decodedTokens);

        TokenDecodingState decodingState = TokenDecodingState.create(buffer);

        while (true) {

            if (!decodingState.canDecode()) {
                break;
            }

            byte type = Decode.asByte(buffer);

            if (type == Terminator.TYPE) {
                decodedTokens.add(Terminator.INSTANCE);
                break;
            }

            if (type == Version.TYPE) {
                decodedTokens.add(Version.decode(decodingState));
                continue;
            }

            if (type == Encryption.TYPE) {
                decodedTokens.add(Encryption.decode(decodingState));
                continue;
            }

            if (type == InstanceValidation.TYPE) {
                decodedTokens.add(InstanceValidation.decode(decodingState));
                continue;
            }

            decodedTokens.add(UnknownToken.decode(type, decodingState));
        }

        // ignore remaining bytes of PreLogin response
        buffer.skipBytes(buffer.readableBytes());

        return prelogin;
    }

    /**
     * @return the tokens.
     */
    public List<? extends Token> getTokens() {
        return this.tokens;
    }

    /**
     * Resolve a {@link Token} given its {@link Class type}.
     *
     * @param tokenType
     * @return
     */
    public <T extends Token> Optional<T> getToken(Class<? extends T> tokenType) {

        Objects.requireNonNull(tokenType, "Token type must not be null");

        for (Token token : this.tokens) {
            if (tokenType.isInstance(token)) {
                return Optional.of(tokenType.cast(token));
            }
        }

        return Optional.empty();
    }

    /**
     * Resolve a {@link Token} given its {@link Class type}.
     *
     * @param tokenType
     * @return
     */
    public <T extends Token> T getRequiredToken(Class<? extends T> tokenType) {

        Objects.requireNonNull(tokenType, "Token type must not be null");

        return getToken(tokenType).orElseThrow(
            () -> new IllegalArgumentException(String.format("No token of type [%s] available", tokenType.getName())));
    }

    private static int getSize(List<? extends Token> tokens) {

        int size = Header.LENGTH;

        for (Token token : tokens) {
            size += token.getTotalLength();
        }

        return size;
    }

    @Override
    public String getName() {
        return "PRELOGIN";
    }

    @Override
    public Publisher<TdsFragment> encode(ByteBufAllocator allocator) {

        Objects.requireNonNull(allocator, "ByteBufAllocator must not be null");

        return Mono.fromSupplier(() -> {

            ByteBuf buffer = allocator.buffer(this.size);

            encode(buffer);

            return new ContextualTdsFragment(HEADER_OPTIONS, buffer);
        });
    }

    /**
     * Encode the {@link Prelogin} request message.
     *
     * @param buffer
     */
    void encode(ByteBuf buffer) {

        int tokenHeaderLength = 0;

        for (Token token : this.tokens) {
            tokenHeaderLength += token.getTokenHeaderLength();
        }

        int position = tokenHeaderLength;
        for (Token token : this.tokens) {

            token.encodeToken(buffer, position);
            position += token.getDataLength();
        }

        for (Token token : this.tokens) {

            token.encodeStream(buffer);
            position += token.getDataLength();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Prelogin)) {
            return false;
        }
        Prelogin prelogin = (Prelogin) o;
        return this.size == prelogin.size &&
            Objects.equals(this.tokens, prelogin.tokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.size, this.tokens);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getName());
        sb.append(" [tokens=").append(this.tokens);
        sb.append(", size=").append(this.size);
        sb.append(']');
        return sb.toString();
    }

    /**
     * Builder for {@link Prelogin}.
     *
     * @author Mark Paluch
     */
    public static class Builder {

        /**
         * Client application thread id;
         */
        private Integer threadId;

        /**
         * Client application trace id (for debugging purposes).
         */
        @Nullable
        private UUID connectionId;

        /**
         * Client application activity id (for debugging purposes).
         */
        @Nullable
        private UUID activityId;

        /**
         * Client application activity sequence (for debugging purposes).
         */
        @Nullable
        private long activitySequence;

        private byte encryption = Encryption.ENCRYPT_OFF;

        private String instanceName = InstanceValidation.MSSQLSERVER_VALUE;

        private Builder() {
        }

        /**
         * Configure the client-side connection {@link UUID}. Typically used for tracing.
         *
         * @param connectionId the connection UUID.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withConnectionId(UUID connectionId) {

            Objects.requireNonNull(connectionId, "ConnectionID must not be null");
            this.connectionId = connectionId;

            return this;
        }

        /**
         * Configure the client-side activity {@link UUID}. Typically used for tracing.
         *
         * @param activityId the activity UUID.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withActivityId(UUID activityId) {

            Objects.requireNonNull(activityId, "Activity ID must not be null");
            this.activityId = activityId;

            return this;
        }

        /**
         * Configure the client-side activity sequence. Typically used for tracing.
         *
         * @param activitySequence the activity sequence.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withActivitySequence(long activitySequence) {

            this.activitySequence = activitySequence;

            return this;
        }

        /**
         * Configure the client-side Thread Id. Typically used for tracing.
         *
         * @param threadId the Thread Id.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withThreadId(int threadId) {

            this.threadId = threadId;

            return this;
        }

        /**
         * Disable encryption.
         *
         * @return {@literal this} {@link Builder}.
         */
        public Builder withEncryptionDisabled() {

            this.encryption = Encryption.ENCRYPT_OFF;

            return this;
        }

        /**
         * Disable encryption by indicating encryption not supported.
         *
         * @return {@literal this} {@link Builder}.
         */
        public Builder withEncryptionNotSupported() {

            this.encryption = Encryption.ENCRYPT_NOT_SUP;

            return this;
        }

        public Builder withInstanceName(String instanceName) {

            Objects.requireNonNull(instanceName, "Instance name must not be null");
            this.instanceName = instanceName;

            return this;
        }

        /**
         * Build the {@link Prelogin} message.
         *
         * @return the {@link Prelogin} message.
         */
        public Prelogin build() {

            List<Token> tokens = new ArrayList<>();

            tokens.add(new Version(0, 0));
            tokens.add(new Encryption(this.encryption));
            tokens.add(new InstanceValidation(this.instanceName));

            if (this.threadId != null) {
                tokens.add(new ThreadId(this.threadId));
            }

            if (this.connectionId != null) {
                tokens.add(new TraceId(this.connectionId, null, 0));
            }

            tokens.add(Terminator.INSTANCE);

            return new Prelogin(tokens);
        }
    }

    /**
     * Pre-Login Token.
     */
    public abstract static class Token {

        private byte type;

        private int length;

        Token(int type, int length) {

            if (type > Byte.MAX_VALUE) {
                throw new IllegalArgumentException("Type " + type + " exceeds byte value");
            }

            this.type = (byte) type;
            this.length = length;
        }

        /**
         * Returns the token type.
         *
         * @return the token type.
         */
        byte getType() {
            return this.type;
        }

        /**
         * Returns the token length.
         *
         * @return the token data length.
         */
        int getLength() {
            return this.length;
        }

        public void encodeToken(ByteBuf buffer, int position) {

            Encode.asByte(buffer, this.type);
            Encode.uShortBE(buffer, position);
            Encode.uShortBE(buffer, this.length);
        }

        public abstract void encodeStream(ByteBuf buffer);

        /**
         * @return total length in bytes (including token header).
         */
        final int getTotalLength() {
            return getDataLength() + getTokenHeaderLength();
        }

        /**
         * @return token header length in bytes.
         */
        int getTokenHeaderLength() {
            return 5;
        }

        /**
         * @return length of data bytes.
         */
        int getDataLength() {
            return this.length;
        }

        /**
         * Apply functional token decoding.
         *
         * @param toDecode  the decoding state.
         * @param validator validator for
         * @param decoder   token decode function.
         * @return the decoded token.
         */
        static <T extends Token> T decode(TokenDecodingState toDecode, LengthValidator validator,
                                          DecodeFunction<T> decoder) {

            Objects.requireNonNull(toDecode, "TokenDecodingState must not be null");
            Objects.requireNonNull(validator, "LengthValidator must not be null");
            Objects.requireNonNull(decoder, "DecodeFunction must not be null");

            ByteBuf buffer = toDecode.buffer;
            short position = buffer.readShort();
            short length = buffer.readShort();

            validator.validate(length);

            buffer.markReaderIndex();

            ByteBuf data = toDecode.readBody(position, length);

            T result = decoder.decode(length, data);

            data.release();

            buffer.resetReaderIndex();

            toDecode.afterTokenDecoded();
            return result;
        }
    }

    /**
     * Terminating token indicating the end of prelogin tokens.
     */
    public static class Terminator extends Token {

        public static final Terminator INSTANCE = new Terminator();

        public static final byte TYPE = (byte) 0xFF;

        public Terminator() {
            super(TYPE, 0);
        }

        @Override
        public void encodeToken(ByteBuf buffer, int position) {
            buffer.writeByte(getType());
        }

        @Override
        public int getTokenHeaderLength() {
            return 1;
        }

        @Override
        public void encodeStream(ByteBuf buffer) {
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" []");
            return sb.toString();
        }
    }

    /**
     * Version information representing the SQL server version.
     */
    public static class Version extends Token {

        public static final byte TYPE = 0x00;

        /**
         * version of the sender.
         */
        private final int version;

        /**
         * sub-build number of the sender
         */
        private final short subbuild;

        public Version(int version, int subbuild) {
            this(version, (byte) subbuild);
        }

        public Version(int version, short subbuild) {

            super(TYPE, 6);

            this.version = version;
            this.subbuild = subbuild;
        }

        /**
         * Decode the {@link Version} token.
         *
         * @param toDecode the current decoding state.
         * @return the decoded {@link Version} token.
         */
        public static Version decode(TokenDecodingState toDecode) {

            return decode(toDecode,
                length -> {
                    if (length != 6) {
                        throw ProtocolException.invalidTds(String.format("Invalid version length: %s", length));
                    }
                },
                (length, body) -> {

                    int major = Decode.asByte(body);
                    int minor = Decode.asByte(body);
                    short build = body.readShort();

                    return new Version(major, build);
                });
        }

        public int getVersion() {
            return this.version;
        }

        public short getSubbuild() {
            return this.subbuild;
        }

        @Override
        public void encodeStream(ByteBuf buffer) {

            Encode.dword(buffer, this.version);
            Encode.shortBE(buffer, this.subbuild);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [version=").append(this.version);
            sb.append(", subbuild=").append(this.subbuild);
            sb.append(']');
            return sb.toString();
        }
    }

    /**
     * Allows validating a remote SQL server instance.
     */
    public static class InstanceValidation extends Token {

        static final String MSSQLSERVER_VALUE = "MSSQLServer";

        public static final byte TYPE = 0x02;

        /**
         * Instance name for validation
         */
        private final byte[] instanceName;

        public InstanceValidation(String instanceName) {
            this(toBytes(instanceName));
        }

        private InstanceValidation(byte[] instanceName) {

            super(TYPE, instanceName.length);
            this.instanceName = instanceName;
        }

        /**
         * Decode the {@link InstanceValidation} token.
         *
         * @param toDecode the current decoding state.
         * @return the decoded {@link InstanceValidation}.
         */
        public static InstanceValidation decode(TokenDecodingState toDecode) {

            return decode(toDecode, LengthValidator.ignore(), (length, body) -> {

                byte[] validation = new byte[length];
                body.readBytes(validation, 0, length);

                return new InstanceValidation(validation);
            });
        }

        @Override
        public void encodeStream(ByteBuf buffer) {
            buffer.writeBytes(this.instanceName);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [instanceName=").append(this.instanceName == null ? "null" : new String(this.instanceName));
            sb.append(']');
            return sb.toString();
        }

        private static byte[] toBytes(String instanceName) {

            Objects.requireNonNull(instanceName, "Instance name must not be null");
            byte[] name = instanceName.getBytes(StandardCharsets.UTF_8);
            byte[] result = new byte[name.length + 1];
            System.arraycopy(name, 0, result, 0, name.length);

            return result;
        }
    }

    /**
     * Allows negotiation of the used encryption mode.
     */
    public static class Encryption extends Token {

        public static final byte TYPE = 0x01;

        /**
         * Disabled encryption but enabled/required for login with credentials.
         */
        public static final byte ENCRYPT_OFF = 0x00;

        /**
         * Encryption enabled.
         */
        public static final byte ENCRYPT_ON = 0x01;

        /**
         * Encryption not supported.
         */
        public static final byte ENCRYPT_NOT_SUP = 0x02;

        /**
         * Encryption required.
         */
        public static final byte ENCRYPT_REQ = 0x03;

        private final byte encryption;

        public Encryption(byte encryption) {

            super(TYPE, 1);

            this.encryption = encryption;
        }

        /**
         * Decode the {@link Encryption} token.
         *
         * @param toDecode
         * @return
         */
        public static Encryption decode(TokenDecodingState toDecode) {

            return decode(toDecode,
                length -> {
                    if (length != 1) {
                        throw ProtocolException.invalidTds(String.format("Invalid encryption length: %s", length));
                    }
                },
                (length, body) -> {

                    byte encryption = Decode.asByte(body);

                    return new Encryption(encryption);
                });
        }

        public byte getEncryption() {
            return this.encryption;
        }

        @Override
        public void encodeStream(ByteBuf buffer) {
            Encode.asByte(buffer, this.encryption);
        }

        /**
         * Returns {@literal true} if the login phase requires a SSL handshake.
         *
         * @return {@literal true} if the login phase requires a SSL handshake.
         */
        public boolean requiresSslHanshake() {
            return getEncryption() == Prelogin.Encryption.ENCRYPT_REQ || getEncryption() == Prelogin.Encryption.ENCRYPT_OFF
                || getEncryption() == Prelogin.Encryption.ENCRYPT_ON;
        }

        public boolean requiresLoginSslHanshake() {
            return getEncryption() == Prelogin.Encryption.ENCRYPT_OFF;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [encryption=").append(this.encryption);
            sb.append(']');
            return sb.toString();
        }
    }

    /**
     * Token that allows associating a client application Thread Id with the connection.
     */
    public static class ThreadId extends Token {

        public static final byte TYPE = 0x03;

        /**
         * Client application thread id;
         */
        private final int threadId;

        public ThreadId(int threadId) {

            super(TYPE, 4);

            this.threadId = threadId;
        }

        @Override
        public void encodeStream(ByteBuf buffer) {
            buffer.writeInt(this.threadId);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [threadId=").append(this.threadId);
            sb.append(']');
            return sb.toString();
        }
    }

    /**
     * TraceId token that allows associating a connectionId/activityId with the connection.
     */
    public static class TraceId extends Token {

        /**
         * Client application trace id (for debugging purposes).
         */
        @Nullable
        private final UUID connectionId;

        /**
         * Client application activity id (for debugging purposes).
         */
        @Nullable
        private final UUID activityId;

        /**
         * Client application activity sequence (for debugging purposes).
         */
        @Nullable
        private final int activitySequence;

        public TraceId(@Nullable UUID connectionId, @Nullable UUID activityId, int activitySequence) {

            super(0x05, 36);

            this.connectionId = connectionId;
            this.activityId = activityId;
            this.activitySequence = activitySequence;
        }

        @Override
        public void encodeStream(ByteBuf buffer) {

            if (this.connectionId != null) {
                buffer.writeLong(this.connectionId.getMostSignificantBits());
                buffer.writeLong(this.connectionId.getLeastSignificantBits());
            } else {
                buffer.writeLong(0);
                buffer.writeLong(0);
            }

            if (this.activityId != null) {
                buffer.writeLong(this.activityId.getMostSignificantBits());
                buffer.writeLong(this.activityId.getLeastSignificantBits());
            } else {
                buffer.writeLong(0);
                buffer.writeLong(0);
            }

            buffer.writeInt(this.activitySequence);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [connectionId=").append(this.connectionId);
            sb.append(", activityId=").append(this.activityId);
            sb.append(", activitySequence=").append(this.activitySequence);
            sb.append(']');
            return sb.toString();
        }
    }

    /**
     * Token placeholder that consumes unknown tokens.
     */
    public static class UnknownToken extends Token {

        public UnknownToken(int type, int length) {
            super(type, length);
        }

        /**
         * Decode the unknown token.
         *
         * @return
         */
        public static UnknownToken decode(byte type, TokenDecodingState toDecode) {

            return decode(toDecode, LengthValidator.ignore(), (length, body) -> {
                return new UnknownToken(type, length);
            });
        }

        @Override
        public void encodeStream(ByteBuf buffer) {
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }

    /**
     * Function to apply decoding.
     *
     * @param <T>
     */
    @FunctionalInterface
    interface DecodeFunction<T> {

        T decode(short length, ByteBuf buffer);
    }

    /**
     * Length validator.
     *
     * @param <T>
     */
    @FunctionalInterface
    interface LengthValidator {

        LengthValidator IGNORE = length -> {
        };

        /**
         * Validate the token data {@code length}.
         *
         * @param length
         * @throws ProtocolException
         */
        void validate(short length);

        /**
         * Returns a {@link LengthValidator} that ignores the length.
         *
         * @return {@link LengthValidator} that ignores the length.
         */
        static LengthValidator ignore() {
            return IGNORE;
        }
    }

    /**
     * Decoding state for Token Stream decoding using positional data length and positional index data reading.
     */
    static class TokenDecodingState {

        ByteBuf buffer;

        int initialReaderIndex;

        int readPositionOffset;

        TokenDecodingState(ByteBuf buffer) {
            this.initialReaderIndex = buffer.readerIndex();
            this.buffer = buffer;
        }

        public static TokenDecodingState create(ByteBuf byteBuf) {
            return new TokenDecodingState(byteBuf);
        }

        public boolean canDecode() {
            return this.buffer.readableBytes() > 0;
        }

        /**
         * Callback to update the state after reading.
         */
        void afterTokenDecoded() {
            this.readPositionOffset = this.buffer.readerIndex() - this.initialReaderIndex;
        }

        /**
         * Read data at {@code position} of {@code length}.
         *
         * @param position position index within the entire buffer.
         * @param length   bytes to read.
         * @return the data bytes.
         */
        ByteBuf readBody(int position, short length) {

            this.buffer.skipBytes(position - 5 /* type 1 byte, position 2 byte, length 2 byte */ - this.readPositionOffset);

            ByteBuf data = this.buffer.alloc().buffer(length);
            this.buffer.readBytes(data, length);

            return data;
        }
    }
}
