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

package io.r2dbc.mssql.message.type;

import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Objects;

/**
 * Type information for a column following the {@code TYPE_INFO} rule
 *
 * @author Mark Paluch
 * @see Collation
 * @see SqlServerType
 * @see Updatability
 */
public interface TypeInformation {

    /**
     * Decode {@link TypeInformation} from the {@code ByteBuf}.
     *
     * @param buffer    the data {@link ByteBuf buffer}.
     * @param readFlags {@literal true} to decode {@code flags} (typically used when not using encryption).
     * @return the decoded {@link TypeInformation}.
     */
    static TypeInformation decode(ByteBuf buffer, boolean readFlags) {
        return TypeBuilder.decode(buffer, readFlags);
    }

    /**
     * Check whether the {@link ByteBuf} contains sufficient readable bytes to decode the {@link TypeInformation}.
     *
     * @param buffer    the data buffer.
     * @param readFlags {@literal true} to parse type flags.
     * @return {@literal true} if the data buffer contains sufficient readable bytes to decode the {@link TypeInformation}.
     */
    static boolean canDecode(ByteBuf buffer, boolean readFlags) {
        return TypeBuilder.canDecode(buffer, readFlags);
    }

    /**
     * Returns the maximal length.
     *
     * @return the maximal length.
     */
    int getMaxLength();

    /**
     * Returns the length {@link LengthStrategy strategy}.
     *
     * @return the length {@link LengthStrategy strategy}.
     */
    LengthStrategy getLengthStrategy();

    /**
     * Returns the precision.
     *
     * @return the precision.
     */
    int getPrecision();

    /**
     * Returns the display size.
     *
     * @return the display size.
     */
    int getDisplaySize();

    /**
     * Returns the scale.
     *
     * @return the scale.
     */
    int getScale();

    /**
     * Returns the  {@link SqlServerType} base type.
     *
     * @return the {@link SqlServerType} base type
     */
    SqlServerType getServerType();

    /**
     * Returns the user type.
     *
     * @returnthe user type.
     */
    int getUserType();

    /**
     * Returns the user type name. Can be {@literal null} if this type information is not related to a user type.
     *
     * @return the user type name.
     */
    @Nullable
    String getUdtTypeName();

    /**
     * Returns the {@link Collation}. Can be {@literal null} if this type information has no collation details.
     *
     * @return the {@link Collation}.
     */
    @Nullable
    Collation getCollation();

    /**
     * Returns the {@link Charset}. Can be {@literal null} if this type information has no collation details.
     *
     * @return the {@link Charset}.
     * @see #getCollation()
     */
    @Nullable
    Charset getCharset();

    /**
     * Returns the server type name.
     *
     * @return the server type name.
     */
    String getServerTypeName();

    /**
     * Returns whether the type is nullable.
     *
     * @return {@literal true} if the type is nullable.
     */
    boolean isNullable();

    /**
     * Returns whether the type is case-sensitive.
     *
     * @return {@literal true} if the type is case-sensitive.
     */
    boolean isCaseSensitive();

    boolean isSparseColumnSet();

    /**
     * Returns whether the type is encrypted.
     *
     * @return {@literal true} if the type is encrypted.
     */
    boolean isEncrypted();

    /**
     * Returns the type {@link Updatability}.
     *
     * @return the type {@link Updatability}.
     */
    Updatability getUpdatability();

    /**
     * Returns whether the type is an identity type.
     *
     * @return {@literal true} if the type is an identity type.
     */
    boolean isIdentity();

    /**
     * Creates a {@link Builder} for {@link TypeInformation}.
     *
     * @return
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link TypeInformation}.
     */
    final class Builder {

        private Charset charset;

        private Collation collation;

        private int displaySize;

        private int flags;

        private LengthStrategy lengthStrategy;

        private int maxLength;

        private int precision;

        private int scale;

        private SqlServerType serverType;

        private String udtTypeName;

        private int userType;

        private Builder() {
        }

        /**
         * Configure the {@link Charset}.
         *
         * @param charset the charset to use.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withCharset(Charset charset) {
            this.charset = Objects.requireNonNull(charset, "Charset must not be null");
            return this;
        }

        /**
         * Configure the {@link Collation}.
         *
         * @param collation the collation to use.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withCollation(Collation collation) {
            this.collation = Objects.requireNonNull(collation, "Collation must not be null");
            return this;
        }

        /**
         * Configure the display size.
         *
         * @param displaySize the display size.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withDisplaySize(int displaySize) {
            this.displaySize = displaySize;
            return this;
        }

        /**
         * Configure flags.
         *
         * @param flags
         * @return {@literal this} {@link Builder}.
         */
        public Builder withFlags(int flags) {
            this.flags = flags;
            return this;
        }

        /**
         * Configure the {@link LengthStrategy}.
         *
         * @param lengthStrategy the display size.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withLengthStrategy(LengthStrategy lengthStrategy) {
            this.lengthStrategy = Objects.requireNonNull(lengthStrategy, "LengthStrategy must not be null");
            return this;
        }

        /**
         * Configure the maximal maxLength.
         *
         * @param flags
         * @return {@literal this} {@link Builder}.
         */
        public Builder withMaxLength(int maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        /**
         * Configure the precision.
         *
         * @param precision
         * @return {@literal this} {@link Builder}.
         */
        public Builder withPrecision(int precision) {
            this.precision = precision;
            return this;
        }

        /**
         * Configure the scale.
         *
         * @param scale
         * @return {@literal this} {@link Builder}.
         */
        public Builder withScale(int scale) {
            this.scale = scale;
            return this;
        }

        /**
         * Configure the {@link SqlServerType}.
         *
         * @param serverType the server type.
         * @return {@literal this} {@link Builder}.
         */
        public Builder withServerType(SqlServerType serverType) {
            this.serverType = Objects.requireNonNull(serverType, "SqlServerType must not be null");

            return this;
        }


        /**
         * Build a new {@link TypeInformation}.
         *
         * @return a new {@link TypeInformation}.
         */
        public TypeInformation build() {

            MutableTypeInformation mutableTypeInformation = new MutableTypeInformation();
            mutableTypeInformation.lengthStrategy = this.lengthStrategy;
            mutableTypeInformation.serverType = this.serverType;
            mutableTypeInformation.flags = this.flags;
            mutableTypeInformation.maxLength = this.maxLength;
            mutableTypeInformation.charset = this.charset;
            mutableTypeInformation.scale = this.scale;
            mutableTypeInformation.userType = this.userType;
            mutableTypeInformation.precision = this.precision;
            mutableTypeInformation.displaySize = this.displaySize;
            mutableTypeInformation.udtTypeName = this.udtTypeName;
            mutableTypeInformation.collation = this.collation;

            return mutableTypeInformation;
        }
    }

    /**
     * Enumeration of updatability constants.
     */
    enum Updatability {

        READ_ONLY(0), READ_WRITE(1), UNKNOWN(2);

        private final byte value;

        Updatability(int value) {
            this.value = (byte) value;
        }

        public byte getValue() {
            return this.value;
        }
    }
}
