/*
 * Copyright 2018-2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

/**
 * Order token to inform the client by which columns the data is ordered.
 *
 * @author Mark Paluch
 */
class OrderToken extends AbstractDataToken {

    public static final byte TYPE = (byte) 0xA9;

    private final List<Integer> orderByColumns;

    /**
     * Create a new {@link OrderToken} given {@link List} of column indexed by which data is ordered.
     *
     * @param orderByColumns must not be null.
     */
    private OrderToken(List<Integer> orderByColumns) {
        super(TYPE);
        this.orderByColumns = orderByColumns;
    }

    /**
     * Decode a {@link OrderToken}.
     *
     * @param buffer the data buffer.
     * @return the {@link OrderToken}.
     */
    public static OrderToken decode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        int length = Decode.uShort(buffer);
        int readerIndex = buffer.readerIndex();

        List<Integer> columns = new ArrayList<>();

        while (buffer.readerIndex() - readerIndex < length) {

            int column = Decode.uShort(buffer);
            columns.add(column);
        }

        return new OrderToken(columns);
    }

    /**
     * Check whether the {@link ByteBuf} can be decoded into an entire {@link OrderToken}.
     *
     * @param buffer the data buffer.
     * @return {@code true} if the buffer contains sufficient data to entirely decode {@link OrderToken}
     */
    public static boolean canDecode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Data buffer must not be null");

        Integer length = Decode.peekUShort(buffer);
        return length != null && buffer.readableBytes() >= length + /* length field */ 2;
    }

    public List<Integer> getOrderByColumns() {
        return this.orderByColumns;
    }

    @Override
    public String getName() {
        return "ORDER";
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [orderByColumns=").append(this.orderByColumns);
        sb.append(']');
        return sb.toString();
    }

}
