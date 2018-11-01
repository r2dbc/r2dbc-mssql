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
import io.r2dbc.mssql.message.tds.Decode;

/**
 * Done in Proc token.
 *
 * @author Mark Paluch
 */
public final class DoneInProcToken extends AbstractDoneToken {

    public static final byte TYPE = (byte) 0xFF;

    /**
     * Creates a new {@link DoneInProcToken}.
     *
     * @param status         status flags, see {@link AbstractDoneToken} constants.
     * @param currentCommand the current command counter.
     * @param rowCount       number of columns if {@link #hasCount()} is set.
     */
    private DoneInProcToken(int status, int currentCommand, long rowCount) {
        super(TYPE, status, currentCommand, rowCount);
    }

    /**
     * Creates a new {@link DoneInProcToken}
     *
     * @param rowCount the row count.
     * @return the {@link DoneInProcToken}.
     */
    public static DoneInProcToken create(long rowCount) {
        return new DoneInProcToken(DONE_FINAL | DONE_COUNT, 0, rowCount);
    }

    /**
     * Decode the {@link DoneInProcToken} response from a {@link ByteBuf}.
     *
     * @param buffer must not be null.
     * @return the decoded {@link DoneInProcToken}.
     */
    public static DoneInProcToken decode(ByteBuf buffer) {

        int status = Decode.uShort(buffer);
        int currentCommand = Decode.uShort(buffer);
        long rowCount = Decode.uLongLong(buffer);

        return new DoneInProcToken(status, currentCommand, rowCount);
    }

    @Override
    public String getName() {
        return "DONEINPROC";
    }
}
