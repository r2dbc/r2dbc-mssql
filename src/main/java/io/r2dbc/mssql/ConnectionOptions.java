/*
 * Copyright 2019-2022 the original author or authors.
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

    private final boolean sendStringParametersAsUnicode;

    ConnectionOptions() {
        this(sql -> false, new DefaultCodecs(), new IndefinitePreparedStatementCache(), true);
    }

    ConnectionOptions(Predicate<String> preferCursoredExecution, Codecs codecs, PreparedStatementCache preparedStatementCache, boolean sendStringParametersAsUnicode) {
        this.preferCursoredExecution = preferCursoredExecution;
        this.codecs = codecs;
        this.preparedStatementCache = preparedStatementCache;
        this.sendStringParametersAsUnicode = sendStringParametersAsUnicode;
    }

    public Codecs getCodecs() {
        return this.codecs;
    }

    public PreparedStatementCache getPreparedStatementCache() {
        return this.preparedStatementCache;
    }

    public boolean prefersCursors(String sql) {
        return this.preferCursoredExecution.test(sql);
    }

    public boolean isSendStringParametersAsUnicode() {
        return this.sendStringParametersAsUnicode;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [preferCursoredExecution=").append(this.preferCursoredExecution);
        sb.append(", codecs=").append(this.codecs);
        sb.append(", preparedStatementCache=").append(this.preparedStatementCache);
        sb.append(", sendStringParametersAsUnicode=").append(this.sendStringParametersAsUnicode);
        sb.append(']');
        return sb.toString();
    }

}
