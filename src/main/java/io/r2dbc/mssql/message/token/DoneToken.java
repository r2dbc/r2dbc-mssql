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
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.tds.Decode;

/**
 * Done token.
 *
 * @author Mark Paluch
 */
public final class DoneToken extends AbstractDoneToken {

    public static final byte TYPE = (byte) 0xFD;

    private static final DoneToken[] INTERMEDIATE = new DoneToken[CACHE_SIZE];

    private static final DoneToken[] MORE_WITH_COUNT_CACHE = new DoneToken[CACHE_SIZE];

    private static final DoneToken[] DONE_WITH_COUNT_CACHE = new DoneToken[CACHE_SIZE];

    private static final DoneToken[] MORE_CACHE = new DoneToken[CACHE_SIZE];

    private static final int DONE_WITH_COUNT = DONE_FINAL | DONE_COUNT;

    private static final int MORE_WITH_COUNT = DONE_MORE | DONE_COUNT;

    private static final int MORE = DONE_MORE;

    static {
        for (int i = 0; i < INTERMEDIATE.length; i++) {
            INTERMEDIATE[i] = new DoneToken(0, 0, i);
            DONE_WITH_COUNT_CACHE[i] = new DoneToken(DONE_WITH_COUNT, 0, i);
            MORE_WITH_COUNT_CACHE[i] = new DoneToken(MORE_WITH_COUNT, 0, i);
            MORE_CACHE[i] = new DoneToken(MORE, 0, i);
        }
    }

    /**
     * Creates a new {@link DoneToken}.
     *
     * @param status         status flags, see {@link AbstractDoneToken} constants.
     * @param currentCommand the current command counter.
     * @param rowCount       number of columns if {@link #hasCount()} is set.
     */
    private DoneToken(int status, int currentCommand, long rowCount) {
        super(TYPE, status, currentCommand, rowCount);
    }

    /**
     * Creates a new {@link DoneToken} indicating a final packet and {@code rowCount}.
     *
     * @param rowCount the row count.
     * @return the {@link DoneToken}.
     * @see #isDone()
     * @see #getRowCount()
     * @see #hasCount()
     */
    public static DoneToken create(long rowCount) {
        return create0(DONE_WITH_COUNT, 0, rowCount);
    }

    /**
     * Creates a new {@link DoneToken} with just a {@code rowCount}.
     *
     * @param rowCount the row count.
     * @return the {@link DoneToken}.
     * @see #getRowCount()
     * @see #hasCount()
     */
    public static DoneToken count(long rowCount) {
        return create0(DONE_COUNT, 0, rowCount);
    }

    /**
     * Creates a new {@link DoneToken} with just a {@code rowCount}.
     *
     * @param rowCount the row count.
     * @return the {@link DoneToken}.
     * @see #getRowCount()
     * @see #hasCount()
     */
    public static DoneToken more(long rowCount) {
        return create0(DONE_MORE | DONE_COUNT, 0, rowCount);
    }

    /**
     * Check whether the the {@link Message} represents a finished {@link DoneToken}.
     *
     * @param message the message to inspect.
     * @return {@literal true} if the {@link Message} represents a finished {@link DoneToken}.
     */
    public static boolean isDone(Message message) {

        if (message instanceof DoneToken) {
            return ((AbstractDoneToken) message).isDone();
        }

        return false;
    }

    /**
     * Decode the {@link DoneToken} response from a {@link ByteBuf}.
     *
     * @param buffer must not be null.
     * @return the decoded {@link DoneToken}.
     */
    public static DoneToken decode(ByteBuf buffer) {

        int status = Decode.uShort(buffer);
        int currentCommand = Decode.uShort(buffer);
        long rowCount = Decode.uLongLong(buffer);

        return create0(status, currentCommand, rowCount);
    }

    private static DoneToken create0(int status, int currentCommand, long rowCount) {

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

        return new DoneToken(status, currentCommand, rowCount);
    }

    @Override
    public String getName() {
        return "DONE";
    }

}
