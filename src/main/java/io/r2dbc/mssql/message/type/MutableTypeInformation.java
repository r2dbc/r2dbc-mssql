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
        return this.maxLength;
    }

    @Override
    public LengthStrategy getLengthStrategy() {
        return this.lengthStrategy;
    }

    @Override
    public int getPrecision() {
        return this.precision;
    }

    @Override
    public int getDisplaySize() {
        return this.displaySize;
    }

    @Override
    public int getScale() {
        return this.scale;
    }

    @Override
    public SqlServerType getServerType() {
        return this.serverType;
    }

    @Override
    public int getUserType() {
        return this.userType;
    }

    @Override
    @Nullable
    public String getUdtTypeName() {
        return this.udtTypeName;
    }

    @Override
    @Nullable
    public Collation getCollation() {
        return this.collation;
    }

    @Override
    @Nullable
    public Charset getCharset() {
        return this.charset;
    }

    @Override
    public String getServerTypeName() {
        return (SqlServerType.UDT == this.serverType) ? this.udtTypeName : this.serverType.toString();
    }

    @Override
    public boolean isNullable() {
        return 0x0001 == (this.flags & 0x0001);
    }

    @Override
    public boolean isCaseSensitive() {
        return 0x0002 == (this.flags & 0x0002);
    }

    @Override
    public boolean isSparseColumnSet() {
        return 0x0400 == (this.flags & 0x0400);
    }

    @Override
    public boolean isEncrypted() {
        return 0x0800 == (this.flags & 0x0800);
    }

    @Override
    public Updatability getUpdatability() {

        int value = (this.flags >> 2) & 0x0003;

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
        return 0x0010 == (this.flags & 0x0010);
    }

    private byte[] getFlags() {
        byte[] f = new byte[2];
        f[0] = (byte) (this.flags & 0xFF);
        f[1] = (byte) ((this.flags >> 8) & 0xFF);
        return f;
    }

    /**
     * Returns true if this type is a textual type with a single-byte character set that is compatible with the 7-bit
     * US-ASCII character set.
     */
    public boolean supportsFastAsciiConversion() {

        switch (this.serverType) {
            case CHAR:
            case VARCHAR:
            case VARCHARMAX:
            case TEXT:
                return this.collation != null && this.collation.hasAsciiCompatibleSBCS();

            default:
                return false;
        }
    }

    @Override
    public String toString() {

        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [maxLength=").append(this.maxLength);
        sb.append(", lengthStrategy=").append(this.lengthStrategy);
        sb.append(", precision=").append(this.precision);
        sb.append(", displaySize=").append(this.displaySize);
        sb.append(", scale=").append(this.scale);
        sb.append(", flags=").append(this.flags);
        sb.append(", serverType=").append(this.serverType);
        sb.append(", userType=").append(this.userType);
        sb.append(", udtTypeName=\"").append(this.udtTypeName).append('\"');
        sb.append(", collation=").append(this.collation);
        sb.append(", charset=").append(this.charset);
        sb.append(']');

        return sb.toString();
    }

}
