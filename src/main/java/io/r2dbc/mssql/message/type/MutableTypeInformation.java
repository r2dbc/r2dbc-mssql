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

import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;

/**
 * Mutable {@link TypeInformation} information for a column.
 *
 * @author Mark Paluch
 */
final class MutableTypeInformation implements TypeInformation {

    int maxLength;

    LengthStrategy lengthStrategy; // Length type (FIXEDLENTYPE, PARTLENTYPE, etc.)

    int precision;

    int displaySize;

    int scale;

    int flags;

    SqlServerType serverType;

    int userType;

    @Nullable
    String udtTypeName;

    // Collation (will be null for non-textual types).
    @Nullable
    Collation collation;

    @Nullable
    Charset charset;

    @Override
    public int getMaxLength() {
        return maxLength;
    }

    @Override
    public LengthStrategy getLengthStrategy() {
        return lengthStrategy;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public int getDisplaySize() {
        return displaySize;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public SqlServerType getServerType() {
        return serverType;
    }

    @Override
    public int getUserType() {
        return userType;
    }

    @Override
    @Nullable
    public String getUdtTypeName() {
        return udtTypeName;
    }

    @Override
    @Nullable
    public Collation getCollation() {
        return collation;
    }

    @Override
    @Nullable
    public Charset getCharset() {
        return charset;
    }

    @Override
    public String getServerTypeName() {
        return (SqlServerType.UDT == serverType) ? udtTypeName : serverType.toString();
    }

    @Override
    public boolean isNullable() {
        return 0x0001 == (flags & 0x0001);
    }

    @Override
    public boolean isCaseSensitive() {
        return 0x0002 == (flags & 0x0002);
    }

    @Override
    public boolean isSparseColumnSet() {
        return 0x0400 == (flags & 0x0400);
    }

    @Override
    public boolean isEncrypted() {
        return 0x0800 == (flags & 0x0800);
    }

    @Override
    public Updatability getUpdatability() {

        int value = (flags >> 2) & 0x0003;

        if (value == 0) {
            return Updatability.READ_ONLY;
        }

        if (value == 1) {
            return Updatability.READ_WRITE;
        }

        return Updatability.UNKNOWN;
    }

    @Override
    public boolean isIdentity() {
        return 0x0010 == (flags & 0x0010);
    }

    private byte[] getFlags() {
        byte[] f = new byte[2];
        f[0] = (byte) (flags & 0xFF);
        f[1] = (byte) ((flags >> 8) & 0xFF);
        return f;
    }

    /**
     * Returns true if this type is a textual type with a single-byte character set that is compatible with the 7-bit
     * US-ASCII character set.
     */
    public boolean supportsFastAsciiConversion() {

        switch (serverType) {
            case CHAR:
            case VARCHAR:
            case VARCHARMAX:
            case TEXT:
                return collation != null && collation.hasAsciiCompatibleSBCS();

            default:
                return false;
        }
    }

    @Override
    public String toString() {

        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [maxLength=").append(maxLength);
        sb.append(", lengthStrategy=").append(lengthStrategy);
        sb.append(", precision=").append(precision);
        sb.append(", displaySize=").append(displaySize);
        sb.append(", scale=").append(scale);
        sb.append(", flags=").append(flags);
        sb.append(", serverType=").append(serverType);
        sb.append(", userType=").append(userType);
        sb.append(", udtTypeName=\"").append(udtTypeName).append('\"');
        sb.append(", collation=").append(collation);
        sb.append(", charset=").append(charset);
        sb.append(']');

        return sb.toString();
    }
}
