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

package io.r2dbc.mssql;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.ParametrizedMssqlStatement.ParsedParameter;
import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.codec.Encoded;
import io.r2dbc.mssql.codec.RpcParameterContext;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.ReturnValue;
import io.r2dbc.mssql.message.token.RpcRequest;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import io.r2dbc.mssql.util.Types;
import io.r2dbc.spi.Parameters;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.r2dbc.mssql.ParametrizedMssqlStatement.ParsedQuery;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Unit tests for {@link ParametrizedMssqlStatement}.
 *
 * @author Mark Paluch
 */
class ParametrizedMssqlStatementUnitTests {

    PreparedStatementCache statementCache = new IndefinitePreparedStatementCache();

    ConnectionOptions connectionOptions = new ConnectionOptions(sql -> true, new DefaultCodecs(), this.statementCache, true);

    @Test
    void shouldSupportSql() {

        assertThat(ParametrizedMssqlStatement.supports("SELECT * from FOO where firstname = @firstname")).isTrue();
        assertThat(ParametrizedMssqlStatement.supports("SELECT * from FOO where firstname =@firstname")).isTrue();
        assertThat(ParametrizedMssqlStatement.supports("SELECT * from FOO where firstname = @foo_bar")).isTrue();

        assertThat(ParametrizedMssqlStatement.supports("SELECT * from FOO where firstname = 'foo'")).isFalse();
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
    void executeWithoutBindingsShouldNotFail() {

        new ParametrizedMssqlStatement(TestClient.NO_OP, this.connectionOptions, "SELECT * from FOO where firstname = @firstname").execute();
        new ParametrizedMssqlStatement(TestClient.NO_OP, this.connectionOptions, "SELECT * FROM users WHERE email = 'name[@]gmail.com'").execute();
    }

    @Test
    void shouldBindParameterByIndex() {

        ParametrizedMssqlStatement statement = new ParametrizedMssqlStatement(TestClient.NO_OP, this.connectionOptions, "SELECT * from FOO where firstname = @firstname");

        statement.bind(0, "name");
        assertThat(statement.getBindings().first().getParameters()).containsKeys("firstname");
    }

    @Test
    void shouldRejectBindIndexOutOfBounds() {

        ParametrizedMssqlStatement statement = new ParametrizedMssqlStatement(TestClient.NO_OP, this.connectionOptions, "SELECT * from FOO where firstname = @firstname");

        assertThatExceptionOfType(IndexOutOfBoundsException.class).isThrownBy(() -> statement.bind(-1, "name"));
        assertThatExceptionOfType(IndexOutOfBoundsException.class).isThrownBy(() -> statement.bind(1, "name"));
    }

    @Test
    void shouldBindParameterByName() {

        ParametrizedMssqlStatement statement = new ParametrizedMssqlStatement(TestClient.NO_OP, this.connectionOptions, "SELECT * from FOO where firstname = @firstname");

        statement.bind("firstname", "firstname");
        assertThat(statement.getBindings().first().getParameters()).containsKeys("firstname");
    }

    @Test
    void shouldBindParameterByNameWithPrefix() {

        ParametrizedMssqlStatement statement = new ParametrizedMssqlStatement(TestClient.NO_OP, this.connectionOptions, "SELECT * from FOO where firstname = @firstname");

        statement.bind("@firstname", "firstname");
        assertThat(statement.getBindings().first().getParameters()).containsKeys("firstname");
    }

    @Test
    void shouldBindParameter() {

        ParametrizedMssqlStatement statement = new ParametrizedMssqlStatement(TestClient.NO_OP, this.connectionOptions, "SELECT * from FOO where amount1 = @amount1 and amount2 = @amount2");

        statement.bind("amount1", Parameters.in(BigDecimal.valueOf(2.1f)));
        statement.bind("amount2", Parameters.in(SqlServerType.MONEY));

        Map<String, Binding.RpcParameter> parameters = statement.getBindings().first().getParameters();
        assertThat(parameters).containsKeys("amount1", "amount2");

        Binding.RpcParameter amount1 = parameters.get("amount1");
        assertThat(amount1.encoded.getDataType()).isEqualTo(TdsDataType.DECIMALN);

        Binding.RpcParameter amount2 = parameters.get("amount2");
        assertThat(amount2.encoded.getDataType()).isEqualTo(TdsDataType.MONEYN);
    }

    @Test
    void shouldRejectBindForUnknownParameters() {

        ParametrizedMssqlStatement statement = new ParametrizedMssqlStatement(TestClient.NO_OP, this.connectionOptions, "SELECT * from FOO where firstname = @firstname");

        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> statement.bind("foo", "name"));
    }

    @Test
    void shouldCachePreparedStatementHandle() {

        Encoded encodedPreparedStatementHandle = new DefaultCodecs().encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), 1);
        ByteBuf value = encodedPreparedStatementHandle.getValue();
        value.skipBytes(1); // skip maxlen byte

        TestClient testClient = TestClient.builder()
            .assertNextRequestWith(it -> {
                assertThat(it).isInstanceOf(RpcRequest.class);
                RpcRequest request = (RpcRequest) it;
                assertThat(request.getProcId()).isEqualTo(RpcRequest.Sp_CursorPrepExec);
            })
            .thenRespond(new ReturnValue(0, null, (byte) 0, Types.integer(),
                    value))
            .build();

        String sql = "SELECT * from FOO where firstname = @firstname";
        ParametrizedMssqlStatement statement = new ParametrizedMssqlStatement(testClient, this.connectionOptions, sql);

        statement.bind("firstname", "");

        Binding binding = statement.getBindings().getCurrent();

        statement.execute().flatMap(MssqlResult::getRowsUpdated).subscribe();

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
        ParametrizedMssqlStatement statement = new ParametrizedMssqlStatement(testClient, this.connectionOptions, sql);

        statement.bind("firstname", "");

        Binding binding = statement.getBindings().getCurrent();

        this.statementCache.putHandle(1, sql, binding);

        statement.execute().subscribe();

        assertThat(this.statementCache.getHandle(sql, binding)).isEqualTo(1);
        assertThat(this.statementCache.size()).isEqualTo(1);
    }

    @Test
    void shouldPropagateError() {

        TestClient testClient = TestClient.builder()
            .assertNextRequestWith(it -> {
                assertThat(it).isInstanceOf(RpcRequest.class);
                RpcRequest request = (RpcRequest) it;
                assertThat(request.getProcId()).isEqualTo(RpcRequest.Sp_ExecuteSql);
            })
            .thenRespond(new ErrorToken(0, 4002, (byte) 0, (byte) 16, "failure", "", "", 0))
            .build();

        String sql = "SELECT * from FOO where firstname = @firstname";
        ParametrizedMssqlStatement statement = new ParametrizedMssqlStatement(testClient, this.connectionOptions, sql).fetchSize(0);

        statement.bind("firstname", "");
        statement.execute().flatMap(MssqlResult::getRowsUpdated).as(StepVerifier::create).verifyError(ProtocolException.class);
    }

}
