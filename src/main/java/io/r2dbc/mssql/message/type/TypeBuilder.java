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
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.message.type.TypeInformation.LengthStrategy;
import io.r2dbc.mssql.message.type.TypeInformation.SqlServerType;
import io.r2dbc.mssql.util.Assert;

import java.util.EnumMap;
import java.util.Map;

/**
 * Builders to parse {@link TypeInformation}.
 *
 * @author Mark Paluch
 */
@SuppressWarnings("unused")
enum TypeBuilder {

    BIT(TdsDataType.BIT1, TypeDecoderStrategies.create(SqlServerType.BIT, 1, // TDS length (bytes)
        1, // precision (max numeric precision, in decimal digits)
        "1".length(), // column display size
        0) // scale
    ),

    BIGINT(TdsDataType.INT8, TypeDecoderStrategies.create(SqlServerType.BIGINT, 8, // TDS length (bytes)
        Long.toString(Long.MAX_VALUE).length(), // precision (max numeric precision, in decimal digits)
        ("-" + Long.toString(Long.MAX_VALUE)).length(), // column display size (includes sign)
        0) // scale
    ),

    INTEGER(TdsDataType.INT4, TypeDecoderStrategies.create(SqlServerType.INTEGER, 4, // TDS length (bytes)
        Integer.toString(Integer.MAX_VALUE).length(), // precision (max numeric precision, in decimal digits)
        ("-" + Integer.toString(Integer.MAX_VALUE)).length(), // column display size (includes sign)
        0) // scale
    ),

    SMALLINT(TdsDataType.INT2, TypeDecoderStrategies.create(SqlServerType.SMALLINT, 2, // TDS length (bytes)
        Short.toString(Short.MAX_VALUE).length(), // precision (max numeric precision, in decimal digits)
        ("-" + Short.toString(Short.MAX_VALUE)).length(), // column display size (includes sign)
        0) // scale
    ),

    TINYINT(TdsDataType.INT1, TypeDecoderStrategies.create(SqlServerType.TINYINT, 1, // TDS length (bytes)
        Byte.toString(Byte.MAX_VALUE).length(), // precision (max numeric precision, in decimal digits)
        Byte.toString(Byte.MAX_VALUE).length(), // column display size (no sign - TINYINT is unsigned)
        0) // scale
    ),

    REAL(TdsDataType.FLOAT4, TypeDecoderStrategies.create(SqlServerType.REAL, 4, // TDS length (bytes)
        7, // precision (max numeric precision, in bits)
        13, // column display size
        0) // scale
    ),

    FLOAT(TdsDataType.FLOAT8, TypeDecoderStrategies.create(SqlServerType.FLOAT, 8, // TDS length (bytes)
        15, // precision (max numeric precision, in bits)
        22, // column display size
        0) // scale
    ),

    SMALLDATETIME(TdsDataType.DATETIME4, TypeDecoderStrategies.create(SqlServerType.SMALLDATETIME, 4, // TDS length (bytes)
        "yyyy-mm-dd hh:mm".length(), // precision (formatted length, in characters, assuming max fractional
        // seconds precision (0))
        "yyyy-mm-dd hh:mm".length(), // column display size
        0) // scale
    ),

    DATETIME(TdsDataType.DATETIME8, TypeDecoderStrategies.create(SqlServerType.DATETIME, 8, // TDS length (bytes)
        "yyyy-mm-dd hh:mm:ss.fff".length(), // precision (formatted length, in characters, assuming max
        // fractional seconds precision)
        "yyyy-mm-dd hh:mm:ss.fff".length(), // column display size
        3) // scale
    ),

    SMALLMONEY(TdsDataType.MONEY4, TypeDecoderStrategies.create(SqlServerType.SMALLMONEY, 4, // TDS length (bytes)
        Integer.toString(Integer.MAX_VALUE).length(), // precision (max unscaled numeric precision, in decimal
        // digits)
        ("-" + "." + Integer.toString(Integer.MAX_VALUE)).length(), // column display size (includes sign and
        // decimal for scale)
        4) // scale
    ),

    MONEY(TdsDataType.MONEY8, TypeDecoderStrategies.create(SqlServerType.MONEY, 8, // TDS length (bytes)
        Long.toString(Long.MAX_VALUE).length(), // precision (max unscaled numeric precision, in decimal digits)
        ("-" + "." + Long.toString(Long.MAX_VALUE)).length(), // column display size (includes sign and decimal
        // for scale)
        4) // scale
    ),

    BITN(TdsDataType.BITN, new AbstractTypeDecoderStrategy(1) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            if (1 != Decode.uByte(buffer)) {
                throw ProtocolException.invalidTds("Invalid mutability for BITN");
            }

            BIT.build(typeInfo, buffer);
            typeInfo.lengthStrategy = LengthStrategy.BYTELENTYPE;
        }
    }
    ),

    INTN(TdsDataType.INTN, new AbstractTypeDecoderStrategy(1) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            int intType = Decode.uByte(buffer);

            switch (intType) {
                case 8:
                    BIGINT.build(typeInfo, buffer);
                    break;
                case 4:
                    INTEGER.build(typeInfo, buffer);
                    break;
                case 2:
                    SMALLINT.build(typeInfo, buffer);
                    break;
                case 1:
                    TINYINT.build(typeInfo, buffer);
                    break;
                default:
                    throw ProtocolException.invalidTds(String.format("Unsupported INTN type %s", intType));
            }

            typeInfo.lengthStrategy = LengthStrategy.BYTELENTYPE;
        }
    }
    ),

    DECIMAL(TdsDataType.DECIMALN, TypeDecoderStrategies.decimalNumeric(SqlServerType.DECIMAL)),
    NUMERIC(TdsDataType.NUMERICN, TypeDecoderStrategies.decimalNumeric(SqlServerType.NUMERIC)),
    FLOATN(TdsDataType.FLOATN, TypeDecoderStrategies.bigOrSmall(FLOAT, REAL)),
    MONEYN(TdsDataType.MONEYN, TypeDecoderStrategies.bigOrSmall(MONEY, SMALLMONEY)),
    DATETIMEN(TdsDataType.DATETIMEN, TypeDecoderStrategies.bigOrSmall(DATETIME, SMALLDATETIME)),

    TIME(TdsDataType.TIMEN, TypeDecoderStrategies.temporal(SqlServerType.TIME)),
    DATETIME2(TdsDataType.DATETIME2N, TypeDecoderStrategies.temporal(SqlServerType.DATETIME2)),
    DATETIMEOFFSET(TdsDataType.DATETIMEOFFSETN, TypeDecoderStrategies.temporal(SqlServerType.DATETIMEOFFSET)),

    DATE(TdsDataType.DATEN, new AbstractTypeDecoderStrategy(0) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {
            typeInfo.serverType = SqlServerType.DATE;
            typeInfo.lengthStrategy = LengthStrategy.BYTELENTYPE;
            typeInfo.maxLength = 3;
            typeInfo.displaySize = typeInfo.precision = "yyyy-mm-dd".length();
        }
    }),

    BIGBINARY(TdsDataType.BIGBINARY, new AbstractTypeDecoderStrategy(2) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            typeInfo.lengthStrategy = LengthStrategy.USHORTLENTYPE;
            typeInfo.maxLength = Decode.uShort(buffer);
            if (typeInfo.maxLength > TypeUtils.SHORT_VARTYPE_MAX_BYTES) {
                throw ProtocolException.invalidTds("Max length exceeds short VARBINARY/VARCHAR type");
            }
            typeInfo.precision = typeInfo.maxLength;
            typeInfo.displaySize = 2 * typeInfo.maxLength;
            typeInfo.serverType = (TypeUtils.UDT_TIMESTAMP == typeInfo.userType) ? SqlServerType.TIMESTAMP : SqlServerType.BINARY;
        }
    }),

    BIGVARBINARY(TdsDataType.BIGVARBINARY, new AbstractTypeDecoderStrategy(2) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            typeInfo.maxLength = Decode.uShort(buffer);

            if (TypeUtils.MAXTYPE_LENGTH == typeInfo.maxLength)// for PLP types
            {
                typeInfo.lengthStrategy = LengthStrategy.PARTLENTYPE;
                typeInfo.serverType = SqlServerType.VARBINARYMAX;
                typeInfo.displaySize = typeInfo.precision = TypeUtils.MAX_VARTYPE_MAX_BYTES;
            } else if (typeInfo.maxLength <= TypeUtils.SHORT_VARTYPE_MAX_BYTES)// for non-PLP types
            {
                typeInfo.lengthStrategy = LengthStrategy.USHORTLENTYPE;
                typeInfo.serverType = SqlServerType.VARBINARY;
                typeInfo.precision = typeInfo.maxLength;
                typeInfo.displaySize = 2 * typeInfo.maxLength;
            } else {
                throw ProtocolException.invalidTds("Cannot parse BIGVARBINARY type info");
            }
        }
    }),

    IMAGE(TdsDataType.IMAGE, new AbstractTypeDecoderStrategy(4) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            typeInfo.lengthStrategy = LengthStrategy.LONGLENTYPE;
            typeInfo.maxLength = Decode.asLong(buffer);

            if (typeInfo.maxLength < 0) {
                throw ProtocolException.invalidTds("Negative IMAGE type length");
            }

            typeInfo.serverType = SqlServerType.IMAGE;
            typeInfo.displaySize = typeInfo.precision = Integer.MAX_VALUE;
        }
    }),

    BIGCHAR(TdsDataType.BIGCHAR, new AbstractTypeDecoderStrategy(7) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            typeInfo.lengthStrategy = LengthStrategy.USHORTLENTYPE;
            typeInfo.maxLength = Decode.uShort(buffer);

            if (typeInfo.maxLength > TypeUtils.SHORT_VARTYPE_MAX_BYTES) {
                throw ProtocolException.invalidTds(String.format("BIGCHAR max length exceeded: %d", typeInfo.maxLength));
            }

            typeInfo.displaySize = typeInfo.precision = typeInfo.maxLength;
            typeInfo.serverType = SqlServerType.CHAR;
            typeInfo.collation = Collation.decode(buffer);
            typeInfo.charset = typeInfo.collation.getCharset();
        }
    }),

    BIGVARCHAR(TdsDataType.BIGVARCHAR, new AbstractTypeDecoderStrategy(7) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            typeInfo.maxLength = Decode.uShort(buffer);

            if (TypeUtils.MAXTYPE_LENGTH == typeInfo.maxLength)// for PLP types
            {
                typeInfo.lengthStrategy = LengthStrategy.PARTLENTYPE;
                typeInfo.serverType = SqlServerType.VARCHARMAX;
                typeInfo.displaySize = typeInfo.precision = TypeUtils.MAX_VARTYPE_MAX_BYTES;
            } else if (typeInfo.maxLength <= TypeUtils.SHORT_VARTYPE_MAX_BYTES)// for non-PLP types
            {
                typeInfo.lengthStrategy = LengthStrategy.USHORTLENTYPE;
                typeInfo.serverType = SqlServerType.VARCHAR;
                typeInfo.displaySize = typeInfo.precision = typeInfo.maxLength;
            } else {
                throw ProtocolException.invalidTds("Cannot parse BIGVARCHAR type info");
            }

            typeInfo.collation = Collation.decode(buffer);
            typeInfo.charset = typeInfo.collation.getCharset();
        }
    }),

    TEXT(TdsDataType.TEXT, new AbstractTypeDecoderStrategy(9) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {
            typeInfo.lengthStrategy = LengthStrategy.LONGLENTYPE;
            typeInfo.maxLength = Decode.asLong(buffer);
            if (typeInfo.maxLength < 0) {
                throw ProtocolException.invalidTds("Negative TEXT type length");
            }
            typeInfo.serverType = SqlServerType.TEXT;
            typeInfo.displaySize = typeInfo.precision = Integer.MAX_VALUE;
            typeInfo.collation = Collation.decode(buffer);
            typeInfo.charset = typeInfo.collation.getCharset();
        }
    }),

    NCHAR(TdsDataType.NCHAR, new AbstractTypeDecoderStrategy(7) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            typeInfo.lengthStrategy = LengthStrategy.USHORTLENTYPE;
            typeInfo.maxLength = Decode.uShort(buffer);
            if (typeInfo.maxLength > TypeUtils.SHORT_VARTYPE_MAX_BYTES || 0 != typeInfo.maxLength % 2) {
                throw ProtocolException.invalidTds("Invalid NCHAR length");
            }
            typeInfo.displaySize = typeInfo.precision = typeInfo.maxLength / 2;
            typeInfo.serverType = SqlServerType.NCHAR;
            typeInfo.collation = Collation.decode(buffer);
            typeInfo.charset = ServerCharset.UNICODE.charset();
        }
    }),

    NVARCHAR(TdsDataType.NVARCHAR, new AbstractTypeDecoderStrategy(7) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            typeInfo.maxLength = Decode.uShort(buffer);
            if (TypeUtils.MAXTYPE_LENGTH == typeInfo.maxLength)// for PLP types
            {
                typeInfo.lengthStrategy = LengthStrategy.PARTLENTYPE;
                typeInfo.serverType = SqlServerType.NVARCHARMAX;
                typeInfo.displaySize = typeInfo.precision = TypeUtils.MAX_VARTYPE_MAX_CHARS;
            } else if (typeInfo.maxLength <= TypeUtils.SHORT_VARTYPE_MAX_BYTES && 0 == typeInfo.maxLength % 2)// for
            // non-PLP
            // types
            {
                typeInfo.lengthStrategy = LengthStrategy.USHORTLENTYPE;
                typeInfo.serverType = SqlServerType.NVARCHAR;
                typeInfo.displaySize = typeInfo.precision = typeInfo.maxLength / 2;
            } else {
                throw ProtocolException.invalidTds("Invalid NVARCHAR length");
            }
            typeInfo.collation = Collation.decode(buffer);
            typeInfo.charset = ServerCharset.UNICODE.charset();
        }
    }),

    NTEXT(TdsDataType.NTEXT, new AbstractTypeDecoderStrategy(9) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            typeInfo.lengthStrategy = LengthStrategy.LONGLENTYPE;
            typeInfo.maxLength = Decode.asLong(buffer);

            if (typeInfo.maxLength < 0) {
                throw ProtocolException.invalidTds("Negative TEXT type length");
            }

            typeInfo.serverType = SqlServerType.NTEXT;
            typeInfo.displaySize = typeInfo.precision = Integer.MAX_VALUE / 2;
            typeInfo.collation = Collation.decode(buffer);
            typeInfo.charset = ServerCharset.UNICODE.charset();
        }
    }),

    GUID(TdsDataType.GUID, new AbstractTypeDecoderStrategy(1) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {

            int maxLength = Decode.uByte(buffer);
            if (maxLength != 16 && maxLength != 0) {
                throw ProtocolException.invalidTds("Negative GUID type length");
            }

            typeInfo.lengthStrategy = LengthStrategy.BYTELENTYPE;
            typeInfo.serverType = SqlServerType.GUID;
            typeInfo.maxLength = maxLength;
            typeInfo.displaySize = 36;
            typeInfo.precision = 36;
        }
    }),

    // TODO: UDT, XML

    SQL_VARIANT(TdsDataType.SQL_VARIANT, new AbstractTypeDecoderStrategy(4) {

        @Override
        public void decode(MutableTypeInformation typeInfo, ByteBuf buffer) {
            typeInfo.lengthStrategy = LengthStrategy.LONGLENTYPE; // sql_variant type should be LONGLENTYPE length.
            typeInfo.maxLength = Decode.asLong(buffer);
            typeInfo.serverType = SqlServerType.SQL_VARIANT;
        }
    });

    private final TdsDataType tdsType;

    private final TypeDecoderStrategy strategy;

    private static final Map<TdsDataType, TypeBuilder> builderMap = new EnumMap<>(TdsDataType.class);

    static {
        for (TypeBuilder builder : TypeBuilder.values()) {
            builderMap.put(builder.getTdsDataType(), builder);
        }
    }

    TypeBuilder(TdsDataType tdsType, TypeDecoderStrategy strategy) {
        this.tdsType = tdsType;
        this.strategy = strategy;
    }

    /**
     * Decode {@link TypeInformation} from the {@code ByteBuf}.
     *
     * @param buffer    the data {@link ByteBuf buffer}.
     * @param readFlags {@literal true} to decode {@code flags} (typically used when not using encryption).
     * @return the decoded {@link TypeInformation}.
     */
    static TypeInformation decode(ByteBuf buffer, boolean readFlags) {

        MutableTypeInformation typeInfo = new MutableTypeInformation();

        // UserType is USHORT in TDS 7.1 and earlier; ULONG in TDS 7.2 and later.
        typeInfo.userType = Decode.intBigEndian(buffer);

        if (readFlags) {
            // Flags (2 bytes)
            typeInfo.flags = Decode.uShort(buffer);
        }

        TdsDataType tdsType = TdsDataType.valueOf(Decode.uByte(buffer));

        TypeBuilder typeBuilder = builderMap.get(tdsType);
        Assert.state(typeBuilder != null, "TypeBuilder for " + tdsType + " not available");
        return typeBuilder.build(typeInfo, buffer);
    }

    /**
     * Check whether the {@link ByteBuf} contains sufficient readable bytes to decode the {@link TypeInformation}.
     *
     * @param buffer  the data buffer.
     * @param boolean {@literal true} to parse type flags.
     * @return {@literal true} if the data buffer contains sufficient readable bytes to decode the {@link TypeInformation}.
     */
    static boolean canDecode(ByteBuf buffer, boolean readFlags) {

        int length = 4 /* user type */ + (readFlags ? 2 : 0);
        int readerIndex = buffer.readerIndex();

        try {
            if (buffer.readableBytes() >= length + 1) {

                buffer.skipBytes(length);

                TdsDataType tdsType = TdsDataType.valueOf(Decode.uByte(buffer));

                TypeBuilder typeBuilder = builderMap.get(tdsType);
                return typeBuilder.strategy.canDecode(buffer);
            }

            return false;
        } finally {
            buffer.readerIndex(readerIndex);
        }
    }

    TdsDataType getTdsDataType() {
        return this.tdsType;
    }

    /**
     * Build the {@link TypeInformation} by parsing details from {@link ByteBuf}.
     *
     * @param typeInfo the type builder.
     * @param buffer   the data buffer.
     * @return the built {@link TypeInformation}.
     * @throws ProtocolException
     */
    MutableTypeInformation build(MutableTypeInformation typeInfo, ByteBuf buffer) throws ProtocolException {

        this.strategy.decode(typeInfo, buffer);

        // Postcondition: SqlServerType and SqlServerLength are initialized

        Assert.state(typeInfo.serverType != null, "Server type must not be null");
        Assert.state(typeInfo.lengthStrategy != null, "Length type must not be null");

        return typeInfo;
    }
}
