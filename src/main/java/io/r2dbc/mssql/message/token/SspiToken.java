/*
 * Copyright 2026 the original author or authors.
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
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.util.Assert;

/**
 * SSPI token returned by the server during the login process.
 *
 * @author Daniel Pachali
 */
public final class SspiToken extends AbstractDataToken {

    public static final byte TYPE = (byte) 0xED;

    private final byte[] sspiBuffer;

    private SspiToken(byte[] sspiBuffer) {

        super(TYPE);
        this.sspiBuffer = Assert.requireNonNull(sspiBuffer, "SSPI buffer must not be null");
    }

    /**
     * Decode a {@link SspiToken}.
     *
     * @param buffer the data buffer.
     * @return the decoded {@link SspiToken}.
     */
    public static SspiToken decode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        int length = Decode.uShort(buffer);
        byte[] sspiBuffer = new byte[length];

        buffer.readBytes(sspiBuffer);

        return new SspiToken(sspiBuffer);
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link SspiToken}.
     *
     * @param buffer the data buffer.
     * @return {@code true} if the buffer contains sufficient data to entirely decode {@link SspiToken}.
     */
    public static boolean canDecode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        Integer length = Decode.peekUShort(buffer);

        return length != null && buffer.readableBytes() >= length + /* length field */ 2;
    }

    public byte[] getSspiBuffer() {
        return this.sspiBuffer;
    }

    @Override
    public String getName() {
        return "SSPI";
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [length=").append(this.sspiBuffer.length);
        sb.append(']');
        return sb.toString();
    }

}
