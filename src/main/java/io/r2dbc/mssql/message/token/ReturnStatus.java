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
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.util.Assert;

/**
 * Return Status token.
 *
 * @author Mark Paluch
 */
public class ReturnStatus extends AbstractDataToken {

    private static final ReturnStatus[] CACHED = new ReturnStatus[128];

    static {

        for (int i = 0; i < CACHED.length; i++) {
            CACHED[i] = new ReturnStatus(i);
        }
    }

    public static final byte TYPE = (byte) 0x79;

    private final int status;

    private ReturnStatus(int status) {

        super(TYPE);
        this.status = status;
    }

    /**
     * Creates a new {@link ReturnStatus}
     *
     * @param status the status value.
     * @return the {@link ReturnStatus}.
     */
    public static ReturnStatus create(int status) {

        if (status >= 0 && status < CACHED.length) {
            return CACHED[status];
        }

        return new ReturnStatus(status);
    }

    /**
     * Decode a {@link ReturnStatus}.
     *
     * @param buffer the data buffer.
     * @return the decoded {@link ReturnStatus}.
     */
    public static ReturnStatus decode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        return ReturnStatus.create(Decode.asLong(buffer));
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into a {@link ReturnStatus}.
     *
     * @param buffer the data buffer.
     * @return {@code true} if the buffer contains sufficient data to entirely decode a {@link ReturnStatus}.
     */
    public static boolean canDecode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        return buffer.readableBytes() >= 5;
    }

    public int getStatus() {
        return this.status;
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String getName() {
        return "RETURNSTATUS";
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [status=").append(this.status);
        sb.append(']');
        return sb.toString();
    }

}
