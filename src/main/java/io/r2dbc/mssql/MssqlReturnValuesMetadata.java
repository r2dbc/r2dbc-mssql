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

import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.message.token.ReturnValue;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.RowMetadata;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Microsoft SQL Server-specific {@link RowMetadata} backed by {@link ReturnValue}.
 *
 * @author Mark Paluch
 */
final class MssqlReturnValuesMetadata extends NamedCollectionSupport<ReturnValue> implements RowMetadata, Collection<String> {

    private final Codecs codecs;

    @Nullable
    private Map<ReturnValue, MssqlColumnMetadata> metadataCache;

    /**
     * Creates a new {@link MssqlReturnValuesMetadata}.
     *
     * @param codecs       the codec registry.
     * @param returnValues collection of {@link ReturnValue}s.
     */
    MssqlReturnValuesMetadata(Codecs codecs, ReturnValue[] returnValues) {
        super(returnValues, toMap(returnValues, ReturnValue::getParameterName), ReturnValue::getParameterName, "return value");
        this.codecs = Assert.requireNonNull(codecs, "Codecs must not be null");
    }

    /**
     * Creates a new {@link MssqlReturnValuesMetadata}.
     *
     * @param codecs         the codec registry.
     * @param columnMetadata the column metadata.
     */
    public static MssqlReturnValuesMetadata create(Codecs codecs, List<ReturnValue> returnValues) {

        Assert.notNull(returnValues, "ReturnValues must not be null");

        return new MssqlReturnValuesMetadata(codecs, returnValues.toArray(new ReturnValue[0]));
    }

    @Override
    public MssqlColumnMetadata getColumnMetadata(int index) {
        if (this.metadataCache == null) {
            this.metadataCache = new HashMap<>();
        }
        return this.metadataCache.computeIfAbsent(this.get(index), returnValue -> new MssqlColumnMetadata(returnValue.asDecodable(), this.codecs));
    }

    @Override
    public MssqlColumnMetadata getColumnMetadata(String identifier) {
        if (this.metadataCache == null) {
            this.metadataCache = new HashMap<>();
        }
        return this.metadataCache.computeIfAbsent(this.get(identifier), returnValue -> new MssqlColumnMetadata(returnValue.asDecodable(), this.codecs));
    }

    @Override
    public List<MssqlColumnMetadata> getColumnMetadatas() {

        if (this.metadataCache == null) {
            this.metadataCache = new HashMap<>();
        }

        List<MssqlColumnMetadata> metadatas = new ArrayList<>(this.getCount());

        for (int i = 0; i < this.getCount(); i++) {

            MssqlColumnMetadata columnMetadata = this.metadataCache.computeIfAbsent(this.get(i), returnValue -> new MssqlColumnMetadata(returnValue.asDecodable(), this.codecs));
            metadatas.add(columnMetadata);
        }

        return metadatas;
    }

    @Override
    public Collection<String> getColumnNames() {
        return this;
    }

    @Override
    ReturnValue find(String name) {

        ReturnValue returnValue = super.find(name);

        if (returnValue == null && !name.startsWith("@")) {
            return super.find("@" + name);
        }

        return returnValue;
    }

}
