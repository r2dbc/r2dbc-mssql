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
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.util.Assert;
import reactor.core.publisher.SynchronousSink;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Tabular response.
 *
 * @author Mark Paluch
 * @see Type#TABULAR_RESULT
 */
public final class Tabular implements Message {

    private final List<? extends DataToken> tokens;

    private Tabular(List<? extends DataToken> tokens) {
        this.tokens = tokens;
    }

    /**
     * Creates a new {@link Tabular}.
     *
     * @param tokens the tokens.
     * @return the tabular token.
     */
    public static Tabular create(DataToken... tokens) {

        Assert.requireNonNull(tokens, "Data tokens must not be null");

        return new Tabular(Arrays.asList(tokens));
    }

    /**
     * Decode the {@link Tabular} response from a {@link ByteBuf}.
     *
     * @param buffer              must not be null.
     * @param encryptionSupported whether encryption is supported.
     * @return the decoded {@link Tabular} response {@link Message}.
     */
    public static Tabular decode(ByteBuf buffer, boolean encryptionSupported) {

        Assert.requireNonNull(buffer, "Buffer must not be null");

        return new Tabular(new TabularDecoder(encryptionSupported).decode(buffer));
    }

    /**
     * Creates a new {@link TabularDecoder}.
     *
     * @param encryptionSupported {@code true} if table column encryption is supported.
     * @return the decoder.
     */
    public static TabularDecoder createDecoder(boolean encryptionSupported) {
        return new TabularDecoder(encryptionSupported);
    }

    /**
     * Creates a new, stateful {@link DecodeFunction}.
     *
     * @param encryptionSupported {@code true} if table column encryption is supported.
     * @return the decoder.
     */
    private static DecodeFunction decodeFunction(boolean encryptionSupported) {

        AtomicReference<ColumnMetadataToken> columns = new AtomicReference<>();

        return (type, buffer) -> {

            if (type == EnvChangeToken.TYPE) {
                return EnvChangeToken.canDecode(buffer) ? EnvChangeToken.decode(buffer) : DecodeFinished.UNABLE_TO_DECODE;
            }

            if (type == FeatureExtAckToken.TYPE) {
                return FeatureExtAckToken.decode(buffer);
            }

            if (type == InfoToken.TYPE) {
                return InfoToken.canDecode(buffer) ? InfoToken.decode(buffer) : DecodeFinished.UNABLE_TO_DECODE;
            }

            if (type == ErrorToken.TYPE) {
                return ErrorToken.canDecode(buffer) ? ErrorToken.decode(buffer) : DecodeFinished.UNABLE_TO_DECODE;
            }

            if (type == LoginAckToken.TYPE) {
                return LoginAckToken.decode(buffer);
            }

            if (type == ColumnMetadataToken.TYPE) {

                if (!ColumnMetadataToken.canDecode(buffer, encryptionSupported)) {
                    return DecodeFinished.UNABLE_TO_DECODE;
                }

                ColumnMetadataToken colMetadataToken = ColumnMetadataToken.decode(buffer, encryptionSupported);

                if (columns.get() == null || colMetadataToken.hasColumns()) {
                    columns.set(colMetadataToken);
                }

                return colMetadataToken;
            }

            if (type == ColInfoToken.TYPE) {
                return ColInfoToken.canDecode(buffer) ? ColInfoToken.decode(buffer) : DecodeFinished.UNABLE_TO_DECODE;
            }

            if (type == TabnameToken.TYPE) {
                return TabnameToken.canDecode(buffer) ? TabnameToken.decode(buffer) : DecodeFinished.UNABLE_TO_DECODE;
            }

            if (type == DoneToken.TYPE) {

                if (DoneToken.canDecode(buffer)) {

                    DoneToken decode = DoneToken.decode(buffer);

                    if (!decode.hasMore()) {
                        columns.set(null);
                    }

                    return decode;
                }

                return DecodeFinished.UNABLE_TO_DECODE;
            }

            if (type == DoneInProcToken.TYPE) {

                if (DoneInProcToken.canDecode(buffer)) {
                    return DoneInProcToken.decode(buffer);
                }

                return DecodeFinished.UNABLE_TO_DECODE;
            }

            if (type == DoneProcToken.TYPE) {

                if (DoneProcToken.canDecode(buffer)) {
                    return DoneProcToken.decode(buffer);
                }

                return DecodeFinished.UNABLE_TO_DECODE;
            }

            if (type == OrderToken.TYPE) {

                if (!OrderToken.canDecode(buffer)) {
                    return DecodeFinished.UNABLE_TO_DECODE;
                }

                return OrderToken.decode(buffer);
            }

            if (type == RowToken.TYPE) {

                ColumnMetadataToken colMetadataToken = columns.get();

                if (!RowToken.canDecode(buffer, colMetadataToken.getColumns())) {
                    return DecodeFinished.UNABLE_TO_DECODE;
                }

                return RowToken.decode(buffer, colMetadataToken.getColumns());
            }

            if (type == NbcRowToken.TYPE) {

                ColumnMetadataToken colMetadataToken = columns.get();

                if (!NbcRowToken.canDecode(buffer, colMetadataToken.getColumns())) {
                    return DecodeFinished.UNABLE_TO_DECODE;
                }

                return NbcRowToken.decode(buffer, colMetadataToken.getColumns());
            }

            if (type == ReturnStatus.TYPE) {
                return ReturnStatus.canDecode(buffer) ? ReturnStatus.decode(buffer) : DecodeFinished.UNABLE_TO_DECODE;
            }

            if (type == ReturnValue.TYPE) {
                return ReturnValue.canDecode(buffer, encryptionSupported) ? ReturnValue.decode(buffer, encryptionSupported) : DecodeFinished.UNABLE_TO_DECODE;
            }

            throw ProtocolException.invalidTds(String.format("Unable to decode unknown token type 0x%02X", type));
        };
    }

    /**
     * @return the tokens.
     */
    public List<? extends DataToken> getTokens() {
        return this.tokens;
    }

    /**
     * Resolve a {@link Prelogin.Token} given its {@link Class type}.
     *
     * @param filter filter that the desired {@link DataToken} must match.
     * @return the lookup result or {@code null} if no {@link DataToken} matches.
     */
    @Nullable
    private DataToken findToken(Predicate<DataToken> filter) {

        Assert.requireNonNull(filter, "Filter must not be null");

        for (DataToken token : this.tokens) {
            if (filter.test(token)) {
                return token;
            }
        }

        return null;
    }

    /**
     * Find a {@link DataToken} by its {@link Class type} and a {@link Predicate}.
     *
     * @param tokenType type of the desired {@link DataToken}.
     * @return the lookup result.
     */
    <T extends DataToken> Optional<T> getToken(Class<? extends T> tokenType) {

        Assert.requireNonNull(tokenType, "Token type must not be null");

        return Optional.ofNullable(findToken(tokenType::isInstance)).map(tokenType::cast);
    }

    /**
     * Find a {@link DataToken} by its {@link Class type} and a {@link Predicate}.
     *
     * @param tokenType type of the desired {@link DataToken}.
     * @param filter    filter that the desired {@link DataToken} must match.
     * @return the lookup result.
     */
    <T extends DataToken> Optional<T> getToken(Class<? extends T> tokenType, Predicate<T> filter) {

        Assert.requireNonNull(tokenType, "Token type must not be null");
        Assert.requireNonNull(filter, "Filter must not be null");

        Predicate<DataToken> predicate = tokenType::isInstance;
        return Optional.ofNullable(findToken(predicate.and(dataToken -> filter.test(tokenType.cast(dataToken)))))
            .map(tokenType::cast);
    }

    /**
     * Find a {@link DataToken} by its {@link Class type} and a {@link Predicate}.
     *
     * @param tokenType type of the desired {@link DataToken}.
     * @return
     * @throws IllegalArgumentException if no token was found.
     */
    <T extends DataToken> T getRequiredToken(Class<? extends T> tokenType) {

        return getToken(tokenType).orElseThrow(
            () -> new IllegalArgumentException(String.format("No token of type [%s] available", tokenType.getName())));
    }

    /**
     * Find a {@link DataToken} by its {@link Class type} and a {@link Predicate}.
     *
     * @param tokenType type of the desired {@link DataToken}.
     * @param filter    filter that the desired {@link DataToken} must match.
     * @return
     * @throws IllegalArgumentException if no token was found.
     */
    <T extends DataToken> T getRequiredToken(Class<? extends T> tokenType, Predicate<T> filter) {

        return getToken(tokenType, filter).orElseThrow(
            () -> new IllegalArgumentException(String.format("No token of type [%s] available", tokenType.getName())));
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [tokens=").append(this.tokens);
        sb.append(']');
        return sb.toString();
    }

    /**
     * Marker {@link DataToken} when decoding is finished.
     */
    enum DecodeFinished implements DataToken {

        /**
         * Decoding is finished.
         */
        FINISHED,

        /**
         * The {@link DecodeFunction} is not able to decode a {@link DataToken} from the given data buffer.
         */
        UNABLE_TO_DECODE;

        @Override
        public byte getType() {
            return (byte) 0xFF;
        }

        @Override
        public String getName() {
            return "DECODE_FINISHED";
        }
    }

    /**
     * A stateful {@link TabularDecoder}. State is required to decode response chunks in multiple attempts/calls to a {@link DecodeFunction}. Typically, state is a previous
     * {@link ColumnMetadataToken column description} for row results.
     *
     * @author Mark Paluch
     */
    public static class TabularDecoder {

        private final DecodeFunction decodeFunction;

        /**
         * @param encryptionSupported whether encryption is supported.
         */
        TabularDecoder(boolean encryptionSupported) {
            this.decodeFunction = Tabular.decodeFunction(encryptionSupported);
        }

        /**
         * Decode the {@link Tabular} response from a {@link ByteBuf}.
         *
         * @param buffer must not be null.
         * @return the decoded {@link Tabular} response {@link Message}.
         */
        public List<DataToken> decode(ByteBuf buffer) {

            Assert.requireNonNull(buffer, "Buffer must not be null");

            List<DataToken> tokens = new ArrayList<>();

            while (true) {

                if (buffer.readableBytes() == 0) {
                    break;
                }

                int readerIndex = buffer.readerIndex();
                byte type = Decode.asByte(buffer);

                DataToken message = this.decodeFunction.tryDecode(type, buffer);

                if (message == DecodeFinished.UNABLE_TO_DECODE) {
                    buffer.readerIndex(readerIndex);
                    break;
                }

                if (message == DecodeFinished.FINISHED) {
                    break;
                }

                tokens.add(message);
            }

            return tokens;
        }

        /**
         * Decode the {@link Tabular} response from a {@link ByteBuf}.
         *
         * @param buffer          must not be null.
         * @param messageConsumer sink to consume decoded frames.
         * @return the decoded {@link Tabular} response {@link Message}.
         */
        public boolean decode(ByteBuf buffer, SynchronousSink<Message> messageConsumer) {

            Assert.requireNonNull(buffer, "Buffer must not be null");

            boolean hasMessages = false;
            while (true) {

                if (buffer.readableBytes() == 0) {
                    break;
                }

                int readerIndex = buffer.readerIndex();
                byte type = Decode.asByte(buffer);

                DataToken message = this.decodeFunction.tryDecode(type, buffer);

                if (message == DecodeFinished.UNABLE_TO_DECODE) {
                    buffer.readerIndex(readerIndex);
                    break;
                }

                if (message == DecodeFinished.FINISHED) {
                    break;
                }

                messageConsumer.next(message);
                hasMessages = true;

            }

            return hasMessages;
        }

    }

    /**
     * Decode function for {@link Tabular} streams. Can be called incrementally until the {@link #tryDecode(byte, ByteBuf) decode method} returns {@link DecodeFinished}.
     */
    @FunctionalInterface
    interface DecodeFunction {

        /**
         * Try to decode a {@link DataToken} from the {@link ByteBuf data buffer}.
         *
         * @param type   token type.
         * @param buffer the data buffer.
         * @return a decoded {@link DataToken} or a {@link DecodeFinished} marker.
         */
        DataToken tryDecode(byte type, ByteBuf buffer);

    }

}
