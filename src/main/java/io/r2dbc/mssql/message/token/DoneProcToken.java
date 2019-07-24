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

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.tds.Decode;

/**
 * Done Proc token.
 *
 * @author Mark Paluch
 */
public final class DoneProcToken extends AbstractDoneToken {

    public static final byte TYPE = (byte) 0xFE;

    private static final DoneProcToken[] INTERMEDIATE = new DoneProcToken[CACHE_SIZE];

    private static final DoneProcToken[] MORE_WITH_COUNT_CACHE = new DoneProcToken[CACHE_SIZE];

    private static final DoneProcToken[] DONE_WITH_COUNT_CACHE = new DoneProcToken[CACHE_SIZE];

    private static final DoneProcToken[] MORE_CACHE = new DoneProcToken[CACHE_SIZE];

    private static final int DONE_WITH_COUNT = DONE_FINAL | DONE_COUNT;

    private static final int MORE_WITH_COUNT = DONE_MORE | DONE_COUNT;

    private static final int MORE = DONE_MORE;

    static {
        for (int i = 0; i < INTERMEDIATE.length; i++) {
            INTERMEDIATE[i] = new DoneProcToken(0, 0, i);
            DONE_WITH_COUNT_CACHE[i] = new DoneProcToken(DONE_WITH_COUNT, 0, i);
            MORE_WITH_COUNT_CACHE[i] = new DoneProcToken(MORE_WITH_COUNT, 0, i);
            MORE_CACHE[i] = new DoneProcToken(MORE, 0, i);
        }
    }

    /**
     * Creates a new {@link DoneProcToken}.
     *
     * @param status         status flags, see {@link AbstractDoneToken} constants.
     * @param currentCommand the current command counter.
     * @param rowCount       number of columns if {@link #hasCount()} is set.
     */
    private DoneProcToken(int status, int currentCommand, long rowCount) {
        super(TYPE, status, currentCommand, rowCount);
    }

    /**
     * Creates a new {@link DoneProcToken} indicating a final packet and {@code rowCount}.
     *
     * @param rowCount the row count.
     * @return the {@link DoneProcToken}.
     */
    public static DoneProcToken create(long rowCount) {
        return create0(DONE_FINAL | DONE_COUNT, 0, rowCount);
    }

    /**
     * Decode the {@link DoneProcToken} response from a {@link ByteBuf}.
     *
     * @param buffer must not be null.
     * @return the decoded {@link DoneProcToken}.
     */
    public static DoneProcToken decode(ByteBuf buffer) {

        int status = Decode.uShort(buffer);
        int currentCommand = Decode.uShort(buffer);
        long rowCount = Decode.uLongLong(buffer);

        return create0(status, currentCommand, rowCount);
    }

    private static DoneProcToken create0(int status, int currentCommand, long rowCount) {

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

        return new DoneProcToken(status, currentCommand, rowCount);
    }

    @Override
    public String getName() {
        return "DONEPROC";
    }
}
