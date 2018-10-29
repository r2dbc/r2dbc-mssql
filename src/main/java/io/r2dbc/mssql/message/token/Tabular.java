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
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.Decode;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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

        Objects.requireNonNull(tokens, "Data tokens must not be null");

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

        Objects.requireNonNull(buffer, "Buffer must not be null");

        return new Tabular(new TabularDecoder(encryptionSupported).decode(buffer));
    }

    /**
     * Creates a new {@link TabularDecoder}.
     *
     * @param encryptionSupported {@literal true} if table column encryption is supported.
     * @return the decoder.
     */
    public static TabularDecoder createDecoder(boolean encryptionSupported) {
        return new TabularDecoder(encryptionSupported);
    }

    /**
     * Creates a new, stateful {@link TabularDecodeFunction}.
     *
     * @param encryptionSupported {@literal true} if table column encryption is supported.
     * @return the decoder.
     */
    private static TabularDecodeFunction decodeFunction(boolean encryptionSupported) {

        AtomicReference<ColumnMetadataToken> columns = new AtomicReference<>();

        return (type, buffer) -> {

            if (type == (byte) 0xFF) {
                return DecodeFinished.INSTANCE;
            }

            if (type == EnvChangeToken.TYPE && EnvChangeToken.canDecode(buffer)) {
                return EnvChangeToken.decode(buffer);
            }

            if (type == FeatureExtAckToken.TYPE) {
                return FeatureExtAckToken.decode(buffer);
            }

            if (type == InfoToken.TYPE && InfoToken.canDecode(buffer)) {
                return InfoToken.decode(buffer);
            }

            if (type == ErrorToken.TYPE && ErrorToken.canDecode(buffer)) {
                return ErrorToken.decode(buffer);
            }

            if (type == LoginAckToken.TYPE) {
                return LoginAckToken.decode(buffer);
            }

            if (type == ColumnMetadataToken.TYPE) {

                // TODO: Chunking support.
                ColumnMetadataToken colMetadataToken = ColumnMetadataToken.decode(buffer, encryptionSupported);
                columns.set(colMetadataToken);

                return colMetadataToken;
            }

            if (type == DoneToken.TYPE && DoneToken.canDecode(buffer)) {
                columns.set(null);
                return DoneToken.decode(buffer);
            }

            if (type == RowToken.TYPE) {

                ColumnMetadataToken colMetadataToken = columns.get();

                if (!RowToken.canDecode(buffer, colMetadataToken.getColumns())) {
                    return null;
                }

                return RowToken.decode(buffer, colMetadataToken.getColumns());
            }

            return null;
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
     * @param filter
     * @return
     */
    @Nullable
    private DataToken findToken(Predicate<DataToken> filter) {

        Objects.requireNonNull(filter, "Filter must not be null");

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
     * @param tokenType
     * @return
     */
    public <T extends DataToken> Optional<T> getToken(Class<? extends T> tokenType) {

        Objects.requireNonNull(tokenType, "Token type must not be null");

        return Optional.ofNullable(findToken(tokenType::isInstance)).map(tokenType::cast);
    }

    /**
     * Find a {@link DataToken} by its {@link Class type} and a {@link Predicate}.
     *
     * @param tokenType
     * @return
     */
    public <T extends DataToken> Optional<T> getToken(Class<? extends T> tokenType, Predicate<T> filter) {

        Objects.requireNonNull(tokenType, "Token type must not be null");
        Objects.requireNonNull(filter, "Filter must not be null");

        Predicate<DataToken> predicate = tokenType::isInstance;
        return Optional.ofNullable(findToken(predicate.and(dataToken -> filter.test(tokenType.cast(dataToken)))))
            .map(tokenType::cast);
    }

    /**
     * Find a {@link DataToken} by its {@link Class type} and a {@link Predicate}.
     *
     * @param tokenType
     * @return
     * @throws IllegalArgumentException if no token was found.
     */
    public <T extends DataToken> T getRequiredToken(Class<? extends T> tokenType) {

        return getToken(tokenType).orElseThrow(
            () -> new IllegalArgumentException(String.format("No token of type [%s] available", tokenType.getName())));
    }

    /**
     * Find a {@link DataToken} by its {@link Class type} and a {@link Predicate}.
     *
     * @param tokenType
     * @param filter
     * @return
     * @throws IllegalArgumentException if no token was found.
     */
    public <T extends DataToken> T getRequiredToken(Class<? extends T> tokenType, Predicate<T> filter) {

        return getToken(tokenType, filter).orElseThrow(
            () -> new IllegalArgumentException(String.format("No token of type [%s] available", tokenType.getName())));
    }

    /**
     * Find a collection of {@link DataToken tokens} given their {@link Class type}.
     *
     * @param tokenType the desired token type.
     * @return List of tokens.
     */
    public <T extends DataToken> List<T> getTokens(Class<T> tokenType) {

        Objects.requireNonNull(tokenType, "Token type must not be null");

        List<T> result = new ArrayList<>();

        for (DataToken token : this.tokens) {
            if (tokenType.isInstance(token)) {
                result.add(tokenType.cast(token));
            }
        }

        return result;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [tokens=").append(this.tokens);
        sb.append(']');
        return sb.toString();
    }

    public enum DecodeFinished implements DataToken {

        INSTANCE;


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
     * Decode function for {@link Tabular} streams.
     */
    @FunctionalInterface
    public interface TabularDecodeFunction {

        @Nullable
        DataToken tryDecode(byte type, ByteBuf buffer);
    }

    /**
     * A stateful {@link TabularDecoder}. State is required to decode response chunks in multiple attempts/calls to a {@link TabularDecodeFunction}. Typically, state is a previous
     * {@link ColumnMetadataToken column description} for row results.
     *
     * @author Mark Paluch
     */
    public static class TabularDecoder {

        private final Tabular.TabularDecodeFunction decodeFunction;

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

            Objects.requireNonNull(buffer, "Buffer must not be null");

            List<DataToken> tokens = new ArrayList<>();

            while (true) {

                if (buffer.readableBytes() == 0) {
                    break;
                }

                int readerIndex = buffer.readerIndex();
                byte type = Decode.asByte(buffer);

                if (type == (byte) 0xFF) {
                    break;
                }

                DataToken message = decodeFunction.tryDecode(type, buffer);

                if (message == null) {
                    buffer.readerIndex(readerIndex);
                    break;
                }

                if (message == Tabular.DecodeFinished.INSTANCE) {
                    break;
                }

                tokens.add(message);
            }

            return tokens;
        }
    }

}
