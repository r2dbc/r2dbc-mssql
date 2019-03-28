/*
 * Copyright 2019 the original author or authors.
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
import io.r2dbc.mssql.codec.DefaultCodecs;

import java.util.function.Predicate;

/**
 * @author Mark Paluch
 */
class ConnectionOptions {

    private final Predicate<String> preferCursoredExecution;

    private final Codecs codecs;

    private final PreparedStatementCache preparedStatementCache;

    ConnectionOptions() {
        this(sql -> false, new DefaultCodecs(), new IndefinitePreparedStatementCache());
    }

    ConnectionOptions(Predicate<String> preferCursoredExecution, Codecs codecs, PreparedStatementCache preparedStatementCache) {
        this.preferCursoredExecution = preferCursoredExecution;
        this.codecs = codecs;
        this.preparedStatementCache = preparedStatementCache;
    }

    public Codecs getCodecs() {
        return codecs;
    }

    public PreparedStatementCache getPreparedStatementCache() {
        return preparedStatementCache;
    }

    public boolean prefersCursors(String sql) {
        return this.preferCursoredExecution.test(sql);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [preferCursoredExecution=").append(preferCursoredExecution);
        sb.append(", codecs=").append(codecs);
        sb.append(", preparedStatementCache=").append(preparedStatementCache);
        sb.append(']');
        return sb.toString();
    }
}
