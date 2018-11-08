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

import io.r2dbc.mssql.PreparedMssqlStatement.ParsedParameter;
import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.codec.Encoded;
import io.r2dbc.mssql.codec.RpcParameterContext;
import io.r2dbc.mssql.message.token.ReturnValue;
import io.r2dbc.mssql.message.token.RpcRequest;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import io.r2dbc.mssql.util.Types;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.r2dbc.mssql.PreparedMssqlStatement.ParsedQuery;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PreparedMssqlStatement}.
 *
 * @author Mark Paluch
 */
class PreparedMssqlStatementUnitTests {

    PreparedStatementCache statementCache = new IndefinitePreparedStatementCache();

    @Test
    void shouldSupportSql() {

        assertThat(PreparedMssqlStatement.supports("SELECT * from FOO where firstname = @firstname")).isTrue();
        assertThat(PreparedMssqlStatement.supports("SELECT * from FOO where firstname =@firstname")).isTrue();
        assertThat(PreparedMssqlStatement.supports("SELECT * from FOO where firstname = @foo_bar")).isTrue();

        assertThat(PreparedMssqlStatement.supports("SELECT * from FOO where firstname = 'foo'")).isFalse();
    }

    @Test
    void shouldParseSql() {

        List<ParsedParameter> variables = ParsedQuery.parse("SELECT * from FOO where firstname = @firstname").getParameters();

        assertThat(variables).hasSize(1);
        assertThat(variables.get(0)).isEqualTo(new ParsedParameter("firstname", 37));


        variables = ParsedQuery.parse("SELECT * from FOO where @p1 = @foo_bar").getParameters();

        assertThat(variables).hasSize(2);
        assertThat(variables.get(0)).isEqualTo(new ParsedParameter("p1", 25));
        assertThat(variables.get(1)).isEqualTo(new ParsedParameter("foo_bar", 31));
    }

    @Test
    void shouldBindParameterByIndex() {

        PreparedMssqlStatement statement = new PreparedMssqlStatement(this.statementCache, TestClient.NO_OP, new DefaultCodecs(), "SELECT * from FOO where firstname = @firstname");

        statement.bind(0, "name");
        assertThat(statement.getBindings().first().getParameters()).containsKeys("firstname");
    }

    @Test
    void shouldRejectBindIndexOutOfBounds() {

        PreparedMssqlStatement statement = new PreparedMssqlStatement(this.statementCache, TestClient.NO_OP, new DefaultCodecs(), "SELECT * from FOO where firstname = @firstname");

        assertThatThrownBy(() -> statement.bind(-1, "name")).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> statement.bind(1, "name")).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void shouldBindParameterByName() {

        PreparedMssqlStatement statement = new PreparedMssqlStatement(this.statementCache, TestClient.NO_OP, new DefaultCodecs(), "SELECT * from FOO where firstname = @firstname");

        statement.bind(0, "firstname");
        assertThat(statement.getBindings().first().getParameters()).containsKeys("firstname");
    }

    @Test
    void shouldRejectBindForUnknownParameters() {

        PreparedMssqlStatement statement = new PreparedMssqlStatement(this.statementCache, TestClient.NO_OP, new DefaultCodecs(), "SELECT * from FOO where firstname = @firstname");

        assertThatThrownBy(() -> statement.bind("foo", "name")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectBatchOperations() {

        PreparedMssqlStatement statement = new PreparedMssqlStatement(this.statementCache, TestClient.NO_OP, new DefaultCodecs(), "SELECT * from FOO where firstname = @firstname");

        statement.bind("firstname", "");
        statement.add();
        statement.bind("firstname", "");

        assertThatThrownBy(statement::execute).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldCachePreparedStatementHandle() {

        Encoded encodedPreparedStatementHandle = new DefaultCodecs().encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), 1);
        encodedPreparedStatementHandle.getValue().skipBytes(1); // skip maxlen byte

        TestClient testClient = TestClient.builder()
            .assertNextRequestWith(it -> {
                assertThat(it).isInstanceOf(RpcRequest.class);
                RpcRequest request = (RpcRequest) it;
                assertThat(request.getProcId()).isEqualTo(RpcRequest.Sp_CursorPrepExec);
            })
            .thenRespond(new ReturnValue(0, null, (byte) 0, Types.integer(),
                encodedPreparedStatementHandle.getValue()))
            .build();

        String sql = "SELECT * from FOO where firstname = @firstname";
        PreparedMssqlStatement statement = new PreparedMssqlStatement(this.statementCache, testClient, new DefaultCodecs(), sql);

        statement.bind("firstname", "");

        Binding binding = statement.getBindings().getCurrent();

        statement.execute().subscribe();

        assertThat(this.statementCache.getHandle(sql, binding)).isEqualTo(1);
    }

    @Test
    void shouldReusePreparedStatementHandle() {

        Encoded cursorId = new DefaultCodecs().encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), 123);
        cursorId.getValue().skipBytes(1); // skip maxlen byte

        TestClient testClient = TestClient.builder()
            .assertNextRequestWith(it -> {
                assertThat(it).isInstanceOf(RpcRequest.class);
                RpcRequest request = (RpcRequest) it;
                assertThat(request.getProcId()).isEqualTo(RpcRequest.Sp_CursorExecute);
            })
            .thenRespond(new ReturnValue(0, null, (byte) 0, Types.integer(),
                cursorId.getValue()))
            .build();

        String sql = "SELECT * from FOO where firstname = @firstname";
        PreparedMssqlStatement statement = new PreparedMssqlStatement(this.statementCache, testClient, new DefaultCodecs(), sql);

        statement.bind("firstname", "");

        Binding binding = statement.getBindings().getCurrent();

        this.statementCache.putHandle(1, sql, binding);

        statement.execute().subscribe();

        assertThat(this.statementCache.getHandle(sql, binding)).isEqualTo(1);
        assertThat(this.statementCache.size()).isEqualTo(1);
    }
}
