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
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Result;

import java.util.Objects;

/**
 * Abstract base class for Done token implementation classes.
 *
 * @author Mark Paluch
 */
public abstract class AbstractDoneToken extends AbstractDataToken implements Result.UpdateCount {

    /**
     * Packet length in bytes.
     */
    public static final int LENGTH = 13;

    /**
     * This DONE is the final DONE in the request.
     */
    static final int DONE_FINAL = 0x00;

    /**
     * This DONE message is not the final DONE message in the response. Subsequent data streams to follow.
     */
    static final int DONE_MORE = 0x01;

    /**
     * An error occurred on the current SQL statement. A preceding ERROR token SHOULD be sent when this bit is set.
     */
    static final int DONE_ERROR = 0x02;

    /**
     * A transaction is in progress.
     */
    static final int DONE_INXACT = 0x04;

    /**
     * The DoneRowCount value is valid. This is used to distinguish between
     * a valid value of 0 for DoneRowCount or just an initialized variable.
     */
    static final int DONE_COUNT = 0x10;

    /**
     * The DONE message is a server acknowledgement of a client ATTENTION message.
     */
    static final int DONE_ATTN = 0x10;

    /**
     * This DONEPROC message is associated with an RPC within a set of batched RPCs. This flag is not set on the last RPC in the RPC batch.
     */
    static final int DONE_RPCINBATCH = 0x80;

    /**
     * Used in place of DONE_ERROR when an error occurred on the current SQL statement, which is severe enough to require the result set, if any, to be discarded.
     */
    static final int DONE_SRVERROR = 0x100;

    static final int CACHE_SIZE = 48;

    private final int status;

    /**
     * The token of the current SQL statement. The token value is provided and controlled by the application layer, which utilizes TDS. The TDS layer does not evaluate the value.
     */
    private final int currentCommand;

    /**
     * The count of rows that were affected by the SQL statement. The value of DoneRowCount is valid if the value of Status includes {@link #DONE_COUNT}.
     */
    private final long rowCount;

    /**
     * Creates a new {@link AbstractDoneToken}.
     *
     * @param type           token type.
     * @param status         status flags, see {@link AbstractDoneToken} constants.
     * @param currentCommand the current command counter.
     * @param rowCount       number of columns if {@link #hasCount()} is set.
     */
    protected AbstractDoneToken(byte type, int status, int currentCommand, long rowCount) {

        super(type);

        this.status = status;
        this.currentCommand = currentCommand;
        this.rowCount = rowCount;
    }

    /**
     * Check whether the {@link Message} represents a finished done token.
     *
     * @param message the message to inspect.
     * @return {@literal true} if the {@link Message} represents a finished done token.
     */
    public static boolean isDone(Message message) {

        if (message instanceof AbstractDoneToken) {
            return ((AbstractDoneToken) message).isDone();
        }

        return false;
    }

    /**
     * Check whether the the {@link Message} has a count.
     *
     * @param message the message to inspect.
     * @return {@literal true} if the {@link Message} has a count.
     */
    public static boolean hasCount(Message message) {

        if (message instanceof AbstractDoneToken) {
            return ((AbstractDoneToken) message).hasCount();
        }

        return false;
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link AbstractDoneToken}.
     *
     * @param buffer the data buffer.
     * @return {@code true} if the buffer contains sufficient data to entirely decode a {@link AbstractDoneToken}.
     * @throws IllegalArgumentException when {@link ByteBuf} is {@code null}.
     */
    public static boolean canDecode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        return buffer.readableBytes() >= LENGTH - 1 /* Decoding always decodes the token type first
        so no need to check the for the type byte */;
    }

    /**
     * Encode this token.
     *
     * @param buffer the data buffer.
     * @throws IllegalArgumentException when {@link ByteBuf} is {@code null}.
     */
    public void encode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        buffer.writeByte(getType());
        Encode.uShort(buffer, getStatus());
        Encode.uShort(buffer, getCurrentCommand());
        Encode.uLongLong(buffer, getRowCount());
    }

    public int getStatus() {
        return this.status;
    }

    /**
     * @return {@code true} if this token indicates the response is done and has no more rows.
     */
    public boolean isDone() {
        return (getStatus() & DONE_MORE) == 0;
    }

    /**
     * @return {@code true} if this token indicates the response is done with a preceding error.
     */
    public boolean isError() {
        return (getStatus() & DONE_ERROR) != 0;
    }

    /**
     * @return {@code true} if this token indicates the response is not done yet and the stream contains more data.
     */
    public boolean hasMore() {
        return (getStatus() & DONE_MORE) != 0;
    }

    /**
     * @return {@code true} if this token contains a row count and {@link #getRowCount()} has a valid value.
     */
    public boolean hasCount() {
        return (getStatus() & DONE_COUNT) != 0;
    }

    /**
     * @return the application-level command counter.
     */
    public int getCurrentCommand() {
        return this.currentCommand;
    }

    /**
     * @return the row count. Only valid if {@link #hasCount()} is set.
     */
    public long getRowCount() {
        return this.rowCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractDoneToken)) {
            return false;
        }
        AbstractDoneToken doneToken = (AbstractDoneToken) o;
        return this.status == doneToken.status &&
            this.currentCommand == doneToken.currentCommand &&
            this.rowCount == doneToken.rowCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.status, this.currentCommand, this.rowCount);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [done=").append(isDone());
        sb.append(", hasCount=").append(hasCount());
        sb.append(", rowCount=").append(getRowCount());
        sb.append(", hasMore=").append(hasMore());
        sb.append(", currentCommand=").append(getCurrentCommand());
        sb.append(']');
        return sb.toString();
    }

    @Override
    public long value() {
        return hasCount() ? getRowCount() : 0;
    }

}
