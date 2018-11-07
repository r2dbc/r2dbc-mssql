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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.r2dbc.mssql.message.type.TdsDataType;

import java.util.Arrays;

import static io.r2dbc.mssql.message.type.TypeInformation.SqlServerType;

/**
 * @author Mark Paluch
 */
public class Encoded extends AbstractReferenceCounted {

    private final TdsDataType dataType;

    private final ByteBuf value;

    protected Encoded(TdsDataType dataType, ByteBuf value) {
        this.dataType = dataType;
        this.value = value;
    }

    public static Encoded of(TdsDataType dataType, ByteBuf value) {
        return new Encoded(dataType, value);
    }

    public TdsDataType getDataType() {
        return dataType;
    }

    public ByteBuf getValue() {
        return value;
    }

    @Override
    public Encoded touch(Object hint) {
        this.value.touch(hint);
        return this;
    }

    @Override
    protected void deallocate() {
        this.value.release();
    }

    /**
     * Returns the formal type such as {@literal INTEGER} or {@literal VARCHAR(255)}
     *
     * @return
     */
    public String getFormalType() {

        for (SqlServerType serverType : SqlServerType.values()) {
            if (Arrays.binarySearch(serverType.getTdsTypes(), this.dataType) >= 0) {
                return serverType.toString();
            }
        }

        throw new IllegalStateException(String.format("Cannot determine a formal type for %s", this.dataType));
    }
}
