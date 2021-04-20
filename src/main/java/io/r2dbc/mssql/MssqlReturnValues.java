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

package io.r2dbc.mssql;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.message.token.ReturnValue;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.util.annotation.Nullable;

import java.util.List;

/**
 * Microsoft SQL Server-specific {@link Row} implementation synthesized from {@link ReturnValue return values}.
 * This object is no longer usable once it was {@link #release() released}.
 *
 * @author Mark Paluch
 * @see #release()
 * @see ReferenceCounted
 */
final class MssqlReturnValues implements Row, Result.Data {

    private static final int STATE_ACTIVE = 0;

    private static final int STATE_RELEASED = 1;

    private final Codecs codecs;

    private final MssqlReturnValuesMetadata metadata;

    private final List<ReturnValue> returnValues;

    private volatile int state = STATE_ACTIVE;

    MssqlReturnValues(Codecs codecs, List<ReturnValue> returnValues, MssqlReturnValuesMetadata metadata) {

        this.codecs = codecs;
        this.metadata = metadata;
        this.returnValues = returnValues;
    }

    /**
     * Create a new {@link MssqlReturnValues}.
     *
     * @param codecs       the codecs to decode tabular data.
     * @param returnValues the return values.
     * @return
     */
    static MssqlReturnValues toReturnValues(Codecs codecs, List<ReturnValue> returnValues) {

        Assert.requireNonNull(codecs, "Codecs must not be null");
        Assert.requireNonNull(returnValues, "ReturnValues must not be null");

        return new MssqlReturnValues(codecs, returnValues, new MssqlReturnValuesMetadata(codecs, returnValues.toArray(new ReturnValue[0])));
    }

    /**
     * Returns the {@link MssqlRowMetadata} associated with this {@link Row}.
     *
     * @return the {@link MssqlRowMetadata} associated with this {@link Row}.
     */
    public MssqlReturnValuesMetadata getMetadata() {
        return this.metadata;
    }

    @Override
    public RowMetadata metadata() {
        return this.metadata;
    }

    @Override
    public <T> T get(int index, Class<T> type) {

        Assert.requireNonNull(type, "Type must not be null");
        requireNotReleased();

        ReturnValue returnValue = this.metadata.get(index);
        return doGet(returnValue, type);
    }

    @Override
    public <T> T get(String name, Class<T> type) {

        Assert.requireNonNull(name, "Name must not be null");
        Assert.requireNonNull(type, "Type must not be null");
        requireNotReleased();

        ReturnValue returnValue = this.metadata.get(name);
        return doGet(returnValue, type);
    }

    @Nullable
    private <T> T doGet(@Nullable ReturnValue returnValue, Class<T> type) {

        if (returnValue == null) {
            return null;
        }

        if (returnValue.getValueType().getServerType() == SqlServerType.SQL_VARIANT) {
            throw new UnsupportedOperationException("sql_variant columns not supported. See https://github.com/r2dbc/r2dbc-mssql/issues/67.");
        }

        ByteBuf value = returnValue.getValue();
        value.markReaderIndex();

        try {
            return this.codecs.decode(value, returnValue.asDecodable(), type);
        } finally {
            value.resetReaderIndex();
        }
    }

    /**
     * Decrement the reference count and release the {@link RowToken} to allow deallocation of underlying memory.
     */
    public void release() {
        requireNotReleased();
        this.state = STATE_RELEASED;
        this.returnValues.forEach(ReturnValue::release);
    }

    private void requireNotReleased() {
        if (this.state == STATE_RELEASED) {
            throw new IllegalStateException("Value cannot be retrieved after row has been released");
        }
    }

}
