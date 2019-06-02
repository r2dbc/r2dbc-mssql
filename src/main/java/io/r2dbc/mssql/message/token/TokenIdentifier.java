/*
 * Copyright 2018-2019 the original author or authors.
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

/**
 * Token identifier.
 */
public final class TokenIdentifier {

    public final byte value;

    private TokenIdentifier(byte value) {
        this.value = value;
    }

    /**
     * @return {@code true} if the token has a zero-length.
     */
    public boolean isZeroLength() {
        return (this.value & 1 << 2) == 0 && (this.value & 1 << 3) != 0;
    }

    /**
     * @return {@code true} if the token has a fixed length.
     * @see #getFixedLength()
     */
    public boolean isFixedLength() {
        return (this.value & 1 << 2) != 0 && (this.value & 1 << 3) != 0;
    }

    /**
     * @return {@code true} if the token has a variable length.
     */
    public boolean isVariableLength() {
        return (this.value & 1 << 2) == 0 && (this.value & 1 << 3) == 0;
    }

    /**
     * @return the length in bytes for fixed-length tokens.
     */
    public int getFixedLength() {

        int len = (this.value >> 4) & 0x3;

        return (int) Math.pow(2, len);
    }

    /**
     * Create a {@link TokenIdentifier} given its {@code value}.
     *
     * @param value
     * @return
     */
    public static TokenIdentifier of(int value) {
        return of((byte) value);
    }

    /**
     * Create a {@link TokenIdentifier} given its {@code value}.
     *
     * @param value
     * @return
     */
    public static TokenIdentifier of(byte value) {
        return new TokenIdentifier(value);
    }
}
