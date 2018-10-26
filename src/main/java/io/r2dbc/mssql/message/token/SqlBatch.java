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
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.header.HeaderOptions;
import io.r2dbc.mssql.message.header.Status;
import io.r2dbc.mssql.message.header.Type;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.TdsFragment;
import io.r2dbc.mssql.message.tds.TdsPackets;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * SQL batch token to execute simple SQL.
 *
 * @author Mark Paluch
 */
public final class SqlBatch implements ClientMessage, TokenStream {

    private final HeaderOptions header;

    private final AllHeaders allHeaders;

    private final String sql;

    /**
     * Creates a new {@link SqlBatch} token.
     *
     * @param outstandingRequests   the number of outstanding requests.
     * @param transactionDescriptor the transaction descriptor (8 byte).
     * @param sql                   the SQL string.
     */
    public SqlBatch(int outstandingRequests, byte[] transactionDescriptor, String sql) {

        Objects.requireNonNull(transactionDescriptor, "Transaction descriptor must not be null");
        Objects.requireNonNull(sql, "SQL must not be null");

        this.header = HeaderOptions.create(Type.SQL_BATCH, Status.empty());
        this.allHeaders = AllHeaders.transactional(transactionDescriptor, outstandingRequests);
        this.sql = sql;
    }

    /**
     * Creates a new {@link SqlBatch}.
     *
     * @param outstandingRequests   the number of outstanding requests.
     * @param transactionDescriptor the transaction descriptor
     * @param sql                   the SQL string.
     * @return the {@link SqlBatch}.
     */
    public static SqlBatch create(int outstandingRequests, TransactionDescriptor transactionDescriptor, String sql) {

        Objects.requireNonNull(transactionDescriptor, "Transaction descriptor must not be null");
        Objects.requireNonNull(sql, "SQL must not be null");

        return new SqlBatch(outstandingRequests, transactionDescriptor.getDescriptor(), sql);
    }

    @Override
    public Publisher<TdsFragment> encode(ByteBufAllocator allocator) {

        return Mono.fromSupplier(() -> {

            int length = allHeaders.getLength() + (sql.length() * 2);

            ByteBuf buffer = allocator.buffer(length);

            encode(buffer);

            return TdsPackets.create(this.header, buffer);
        });
    }

    void encode(ByteBuf buffer) {

        this.allHeaders.encode(buffer);
        Encode.unicodeStream(buffer, this.sql);
    }

    public String getSql() {
        return this.sql;
    }

    @Override
    public String getName() {
        return "SQLBatch";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SqlBatch)) {
            return false;
        }
        SqlBatch batch = (SqlBatch) o;
        return Objects.equals(allHeaders, batch.allHeaders) &&
            Objects.equals(sql, batch.sql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(allHeaders, sql);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [sql='").append(sql).append('\"');
        sb.append(']');
        return sb.toString();
    }
}
