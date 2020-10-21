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

package io.r2dbc.mssql.message.type;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.ProtocolException;

/**
 * Typical type parsing strategies.
 *
 * @author Mark Paluch
 */
interface TypeDecoderStrategies {

    /**
     * Strategy using a byte length ({@code 8} or {@code 4}) for big or small type parsing.
     *
     * @param big   big type.
     * @param small small type.
     * @return the decoder strategy.
     */
    static TypeDecoderStrategy bigOrSmall(TypeBuilder big, TypeBuilder small) {
        return new BigOrSmallByteLenStrategy(big, small);
    }

    /**
     * Strategy for {@link LengthStrategy#FIXEDLENTYPE}.
     *
     * @param serverType  the server data type.
     * @param maxLength   actual length of the type.
     * @param precision   the precision.
     * @param displaySize
     * @param scale
     * @return the decoder strategy.
     */
    static TypeDecoderStrategy create(SqlServerType serverType, int maxLength, int precision, int displaySize, int scale) {
        return new FixedLenStrategy(serverType, maxLength, precision, displaySize, scale);
    }

    /**
     * Strategy for decimal numbers using {@link LengthStrategy#BYTELENTYPE}.
     *
     * @param serverType the server data type.
     * @return the decoder strategy.
     */
    static TypeDecoderStrategy decimalNumeric(SqlServerType serverType) {
        return new DecimalNumericStrategy(serverType);
    }

    /**
     * Strategy for temporal types.
     *
     * @param serverType the server data type.
     * @return the decoder strategy.
     */
    static TypeDecoderStrategy temporal(SqlServerType serverType) {
        return new KatmaiScaledTemporalStrategy(serverType);
    }

    /**
     * Strategy for {@link LengthStrategy#FIXEDLENTYPE}.
     */
    class FixedLenStrategy extends AbstractTypeDecoderStrategy {

        private final SqlServerType serverType;

        private final int maxLength;

        private final int precision;

        private final int displaySize;

        private final int scale;

        FixedLenStrategy(SqlServerType serverType, int maxLength, int precision, int displaySize, int scale) {
            super(0);
            this.serverType = serverType;
            this.maxLength = maxLength;
            this.precision = precision;
            this.displaySize = displaySize;
            this.scale = scale;
        }

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            typeInfo.lengthStrategy = LengthStrategy.FIXEDLENTYPE;
            typeInfo.serverType = this.serverType;
            typeInfo.maxLength = this.maxLength;
            typeInfo.precision = this.precision;
            typeInfo.displaySize = this.displaySize;
            typeInfo.scale = this.scale;
        }

    }

    /**
     * Strategy for decimal numbers using {@link LengthStrategy#BYTELENTYPE}.
     */
    class DecimalNumericStrategy extends AbstractTypeDecoderStrategy {

        private final SqlServerType serverType;

        DecimalNumericStrategy(SqlServerType serverType) {
            super(3);
            this.serverType = serverType;
        }

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            int maxLength = Decode.uByte(buffer);
            int precision = Decode.uByte(buffer);
            int scale = Decode.uByte(buffer);

            if (maxLength > 17) {
                throw ProtocolException.invalidTds(String.format("Invalid maximal length for decimal number type: %d", maxLength));
            }

            typeInfo.lengthStrategy = LengthStrategy.BYTELENTYPE;
            typeInfo.serverType = this.serverType;
            typeInfo.maxLength = maxLength;
            typeInfo.precision = precision;
            typeInfo.displaySize = precision + 2;
            typeInfo.scale = scale;
        }

    }

    /**
     * Strategy using a byte length ({@code 8} or {@code 4}) for big or small type parsing.
     */
    class BigOrSmallByteLenStrategy extends AbstractTypeDecoderStrategy {

        private final TypeBuilder bigBuilder;

        private final TypeBuilder smallBuilder;

        BigOrSmallByteLenStrategy(TypeBuilder bigBuilder, TypeBuilder smallBuilder) {
            super(1);
            this.bigBuilder = bigBuilder;
            this.smallBuilder = smallBuilder;
        }

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            int length = Decode.uByte(buffer);

            switch (length) // maxLength
            {
                case 8:
                    this.bigBuilder.build(typeInfo, buffer);
                    break;
                case 4:
                    this.smallBuilder.build(typeInfo, buffer);
                    break;
                default:
                    throw ProtocolException.invalidTds(String.format("Unsupported length for Big/Small strategy: %d", length));
            }

            typeInfo.lengthStrategy = LengthStrategy.BYTELENTYPE;
        }

    }

    /**
     * Strategy for temporal types.
     */
    class KatmaiScaledTemporalStrategy extends AbstractTypeDecoderStrategy {

        private final SqlServerType serverType;

        KatmaiScaledTemporalStrategy(SqlServerType serverType) {
            super(1);
            this.serverType = serverType;
        }

        private int getPrecision(String baseFormat, int scale) {
            // For 0-scale temporal, there is no '.' after the seconds component because there are no sub-seconds.
            // Example: 12:34:56.12134 includes a '.', but 12:34:56 doesn't
            return baseFormat.length() + ((scale > 0) ? (1 + scale) : 0);
        }

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            typeInfo.scale = Decode.uByte(buffer);
            if (typeInfo.scale > TypeUtils.MAX_FRACTIONAL_SECONDS_SCALE) {
                throw ProtocolException.invalidTds(String.format("Unsupported temporal scale: %d", typeInfo.scale));
            }

            switch (this.serverType) {
                case TIME:
                    typeInfo.precision = getPrecision("hh:mm:ss", typeInfo.scale);
                    typeInfo.maxLength = TypeUtils.getTimeValueLength(typeInfo.scale);
                    break;

                case DATETIME2:
                    typeInfo.precision = getPrecision("yyyy-mm-dd hh:mm:ss", typeInfo.scale);
                    typeInfo.maxLength = TypeUtils.getDateTimeValueLength(typeInfo.scale);
                    break;

                case DATETIMEOFFSET:
                    typeInfo.precision = getPrecision("yyyy-mm-dd hh:mm:ss +HH:MM", typeInfo.scale);
                    typeInfo.maxLength = TypeUtils.getDatetimeoffsetValueLength(typeInfo.scale);
                    break;

                default:
                    throw ProtocolException.invalidTds(String.format("Unexpected SQL Server type: %s", this.serverType));
            }

            typeInfo.lengthStrategy = LengthStrategy.BYTELENTYPE;
            typeInfo.serverType = this.serverType;
            typeInfo.displaySize = typeInfo.precision;
        }

    }

}
