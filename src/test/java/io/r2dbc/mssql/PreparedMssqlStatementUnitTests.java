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

package io.r2dbc.mssql;

import io.r2dbc.mssql.PreparedMssqlStatement.ParsedVariable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PreparedMssqlStatement}.
 *
 * @author Mark Paluch
 */
class PreparedMssqlStatementUnitTests {

    @Test
    void shouldSupportSql() {

        assertThat(PreparedMssqlStatement.supports("SELECT * from FOO where firstname = @firstname")).isTrue();
        assertThat(PreparedMssqlStatement.supports("SELECT * from FOO where firstname =@firstname")).isTrue();
        assertThat(PreparedMssqlStatement.supports("SELECT * from FOO where firstname = @foo_bar")).isTrue();

        assertThat(PreparedMssqlStatement.supports("SELECT * from FOO where firstname = 'foo'")).isFalse();
    }

    @Test
    void shouldParseSql() {

        List<ParsedVariable> variables = PreparedMssqlStatement.parse("SELECT * from FOO where firstname = @firstname").getVariables();

        assertThat(variables).hasSize(1);
        assertThat(variables.get(0)).isEqualTo(new ParsedVariable("firstname", 37));


        variables = PreparedMssqlStatement.parse("SELECT * from FOO where @p1 = @foo_bar").getVariables();

        assertThat(variables).hasSize(2);
        assertThat(variables.get(0)).isEqualTo(new ParsedVariable("p1", 25));
        assertThat(variables.get(1)).isEqualTo(new ParsedVariable("foo_bar", 31));
    }
}
