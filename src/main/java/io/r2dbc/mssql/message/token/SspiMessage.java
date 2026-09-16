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
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.TdsFragment;
import io.r2dbc.mssql.message.tds.TdsPackets;
import io.r2dbc.mssql.util.Assert;

/**
 * SSPI message sent by the client during integrated authentication.
 *
 * @author Daniel Pachali
 */
public final class SspiMessage implements ClientMessage, TokenStream {

    private static final HeaderOptions HEADER = HeaderOptions.create(Type.SSPI, Status.empty());

    private final byte[] sspiBuffer;

    private SspiMessage(byte[] sspiBuffer) {
        this.sspiBuffer = Assert.requireNonNull(sspiBuffer, "SSPI buffer must not be null");
    }

    /**
     * Creates a new {@link SspiMessage}.
     *
     * @param sspiBuffer the SSPI/SPNEGO authentication token.
     * @return the {@link SspiMessage}.
     */
    public static SspiMessage create(byte[] sspiBuffer) {
        return new SspiMessage(sspiBuffer);
    }

    @Override
    public TdsFragment encode(ByteBufAllocator allocator, int packetSize) {

        Assert.requireNonNull(allocator, "ByteBufAllocator must not be null");

        ByteBuf buffer = allocator.buffer(this.sspiBuffer.length);
        buffer.writeBytes(this.sspiBuffer);

        return TdsPackets.create(HEADER, buffer);
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
