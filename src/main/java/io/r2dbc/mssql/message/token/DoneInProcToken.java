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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.tds.Decode;

/**
 * Done in Proc token.
 *
 * @author Mark Paluch
 */
public final class DoneInProcToken extends AbstractDoneToken {

    public static final byte TYPE = (byte) 0xFF;

    private static final DoneInProcToken[] INTERMEDIATE = new DoneInProcToken[CACHE_SIZE];

    private static final DoneInProcToken[] MORE_WITH_COUNT_CACHE = new DoneInProcToken[CACHE_SIZE];

    private static final DoneInProcToken[] DONE_WITH_COUNT_CACHE = new DoneInProcToken[CACHE_SIZE];

    private static final DoneInProcToken[] MORE_CACHE = new DoneInProcToken[CACHE_SIZE];

    private static final int DONE_WITH_COUNT = DONE_FINAL | DONE_COUNT;

    private static final int MORE_WITH_COUNT = DONE_MORE | DONE_COUNT;

    private static final int MORE = DONE_MORE;

    static {
        for (int i = 0; i < INTERMEDIATE.length; i++) {
            INTERMEDIATE[i] = new DoneInProcToken(0, 0, i);
            DONE_WITH_COUNT_CACHE[i] = new DoneInProcToken(DONE_WITH_COUNT, 0, i);
            MORE_WITH_COUNT_CACHE[i] = new DoneInProcToken(MORE_WITH_COUNT, 0, i);
            MORE_CACHE[i] = new DoneInProcToken(MORE, 0, i);
        }
    }

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
     * Creates a new {@link DoneInProcToken} indicating a final packet and {@code rowCount}.
     *
     * @param rowCount the row count.
     * @return the {@link DoneInProcToken}.
     */
    public static DoneInProcToken create(long rowCount) {
        return create0(DONE_WITH_COUNT, 0, rowCount);
    }

    /**
     * Check whether the the {@link Message} represents a finished {@link DoneInProcToken}.
     *
     * @param message the message to inspect.
     * @return {@literal true} if the {@link Message} represents a finished {@link DoneInProcToken}.
     */
    public static boolean isDone(Message message) {

        if (message instanceof DoneInProcToken) {
            return ((DoneInProcToken) message).isDone();
        }

        return false;
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

        return create0(status, currentCommand, rowCount);
    }

    private static DoneInProcToken create0(int status, int currentCommand, long rowCount) {

        if (rowCount >= 0 && rowCount < CACHE_SIZE) {

            switch (status) {
                case 0:
                    return INTERMEDIATE[(int) rowCount];
                case DONE_WITH_COUNT:
                    return DONE_WITH_COUNT_CACHE[(int) rowCount];
                case MORE_WITH_COUNT:
                    return MORE_WITH_COUNT_CACHE[(int) rowCount];
                case MORE:
                    return MORE_CACHE[(int) rowCount];
            }
        }

        return new DoneInProcToken(status, currentCommand, rowCount);
    }

    @Override
    public String getName() {
        return "DONEINPROC";
    }

}
