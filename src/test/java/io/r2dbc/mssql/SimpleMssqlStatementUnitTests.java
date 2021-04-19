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

import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.client.ConnectionContext;
import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.tds.ServerCharset;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.DataToken;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.message.token.RowTokenFactory;
import io.r2dbc.mssql.message.token.RpcRequest;
import io.r2dbc.mssql.message.token.SqlBatch;
import io.r2dbc.mssql.message.token.Tabular;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.LengthStrategy;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static io.r2dbc.mssql.message.type.TypeInformation.Builder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MssqlResult}.
 *
 * @author Mark Paluch
 */
class SimpleMssqlStatementUnitTests {

    static final Column[] COLUMNS = Arrays.asList(createColumn(0, "employee_id", SqlServerType.TINYINT, 1, LengthStrategy.FIXEDLENTYPE, null),
        createColumn(1, "last_name", SqlServerType.NVARCHAR, 100, LengthStrategy.USHORTLENTYPE, ServerCharset.UNICODE.charset()),

        createColumn(2, "first_name", SqlServerType.VARCHAR, 50, LengthStrategy.USHORTLENTYPE, ServerCharset.CP1252.charset()),

        createColumn(3, "salary", SqlServerType.MONEY, 8, LengthStrategy.BYTELENTYPE, null)).toArray(new Column[0]);

    static final ConnectionOptions OPTIONS = new ConnectionOptions();

    @Test
    void shouldReportNumberOfAffectedRows() {

        SqlBatch batch = SqlBatch.create(1, TransactionDescriptor.empty(), "SELECT * FROM foo");

        ColumnMetadataToken columns = ColumnMetadataToken.create(COLUMNS);

        Tabular tabular = Tabular.create(columns, DoneToken.create(1));

        TestClient client = TestClient.builder().expectRequest(batch).thenRespond(tabular.getTokens().toArray(new DataToken[0])).build();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, OPTIONS, "SELECT * FROM foo").fetchSize(0);

        statement.execute()
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
    }

    @Test
    void shouldReturnColumnData() {

        TestClient client = simpleResultAndCount();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, OPTIONS, "SELECT * FROM foo").fetchSize(0);

        statement.execute()
            .flatMap(result -> result.map((row, md) -> {

                Map<String, Object> rowData = new HashMap<>();
                for (ColumnMetadata column : md.getColumnMetadatas()) {
                    rowData.put(column.getName(), row.get(column.getName()));
                }

                return rowData;
            }))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> {

                assertThat(actual).containsEntry("first_name", "mark").containsEntry("last_name", "paluch");

            })
            .verifyComplete();
    }

    @Test
    void shouldFlatMapResults() {

        TestClient client = simpleResultAndCount();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, OPTIONS, "SELECT * FROM foo").fetchSize(0);

        statement.execute()
            .flatMap(result -> result.flatMap(segment -> Mono.just(segment.toString())))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> {
                assertThat(actual).contains("MssqlRow");
            })
            .consumeNextWith(actual -> {
                assertThat(actual).contains("DoneToken");
            })
            .verifyComplete();
    }

    @Test
    void shouldFilterAndFlatMapCount() {

        TestClient client = simpleResultAndCount();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, OPTIONS, "SELECT * FROM foo").fetchSize(0);

        statement.execute()
            .flatMap(result -> result.filter(Result.UpdateCount.class::isInstance).flatMap(segment -> Mono.just(segment.toString())))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> {
                assertThat(actual).contains("DoneToken");
            })
            .verifyComplete();
    }

    @Test
    void shouldFilterAndFlatMapData() {

        TestClient client = simpleResultAndCount();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, OPTIONS, "SELECT * FROM foo").fetchSize(0);

        statement.execute()
            .flatMap(result -> result.filter(Result.Data.class::isInstance).flatMap(segment -> {

                Result.Data data = (Result.Data) segment;

                Map<String, Object> rowData = new HashMap<>();
                for (ColumnMetadata column : data.metadata().getColumnMetadatas()) {
                    rowData.put(column.getName(), data.get(column.getName()));
                }

                return Mono.just(rowData);
            }))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> {
                assertThat(actual).containsEntry("first_name", "mark").containsEntry("last_name", "paluch");
            })
            .verifyComplete();
    }

    @Test
    void shouldFilterError() {

        SqlBatch batch = SqlBatch.create(1, TransactionDescriptor.empty(), "SELECT * FROM foo");

        TestClient client = TestClient.builder().expectRequest(batch).thenRespond(new ErrorToken(10, 10, (byte) 1, (byte) 1, "foo", null, null, 0)).build();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, OPTIONS, "SELECT * FROM foo").fetchSize(0);

        statement.execute()
            .flatMap(result -> result.filter(Result.Data.class::isInstance).getRowsUpdated())
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void shouldFlatMapErrorMessage() {

        SqlBatch batch = SqlBatch.create(1, TransactionDescriptor.empty(), "SELECT * FROM foo");

        TestClient client = TestClient.builder().expectRequest(batch).thenRespond(new ErrorToken(10, 10, (byte) 1, (byte) 2, "foo", null, null, 0)).build();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, OPTIONS, "SELECT * FROM foo").fetchSize(0);

        statement.execute()
            .flatMap(result -> result.filter(Result.Message.class::isInstance).flatMap(segment -> {
                return Mono.just(segment).cast(Result.Message.class);
            }))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> {

                assertThat(actual.errorCode()).isEqualTo(10);
                assertThat(actual.message()).isEqualTo("foo");
                assertThat(actual.sqlState()).isEqualTo("S0001");
                assertThat(actual.severity()).isEqualTo(Result.Message.Severity.ERROR);
                assertThat(actual.exception()).isInstanceOf(R2dbcNonTransientResourceException.class);

            })
            .verifyComplete();
    }

    private TestClient simpleResultAndCount() {

        SqlBatch batch = SqlBatch.create(1, TransactionDescriptor.empty(), "SELECT * FROM foo");

        ColumnMetadataToken columns = ColumnMetadataToken.create(COLUMNS);

        RowToken rowToken = RowTokenFactory.create(columns, buffer -> {

            Encode.asByte(buffer, 1);
            Encode.uString(buffer, "paluch", ServerCharset.UNICODE.charset());
            Encode.uString(buffer, "mark", ServerCharset.CP1252.charset());

            //money/salary
            Encode.asByte(buffer, 8);
            Encode.money(buffer, new BigDecimal("50.0000").unscaledValue());
        });

        Tabular tabular = Tabular.create(columns, rowToken, DoneToken.create(1));

        return TestClient.builder().expectRequest(batch).thenRespond(tabular.getTokens().toArray(new DataToken[0])).build();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldPreferCursoredExecution() {

        Client client = mockClient();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, OPTIONS, "SELECT * FROM foo");

        statement.execute().as(StepVerifier::create)
            .verifyComplete();

        ArgumentCaptor<Publisher<Message>> captor = ArgumentCaptor.forClass(Publisher.class);

        verify(client).exchange((Publisher) captor.capture(), any(Predicate.class));

        StepVerifier.create(captor.getValue())
            .consumeNextWith(it -> assertThat(it)
                .isInstanceOf(RpcRequest.class))
            .thenCancel()
            .verify();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldForceDirectExecution() {

        Client client = mockClient();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, OPTIONS, "SELECT * FROM foo").fetchSize(0);

        statement.execute().as(StepVerifier::create)
            .verifyComplete();

        ArgumentCaptor<Mono<ClientMessage>> captor = ArgumentCaptor.forClass(Mono.class);

        verify(client).exchange(captor.capture(), any(Predicate.class));
        assertThat(captor.getValue().block()).isInstanceOf(SqlBatch.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldPreferDirectExecution() {

        Client client = mockClient();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, OPTIONS, "INSERT INTO");

        statement.execute().as(StepVerifier::create)
            .verifyComplete();

        ArgumentCaptor<Mono<ClientMessage>> captor = ArgumentCaptor.forClass(Mono.class);

        verify(client).exchange(captor.capture(), any(Predicate.class));
        assertThat(captor.getValue().block()).isInstanceOf(SqlBatch.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldForceCursoredExecution() {

        Client client = mockClient();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, OPTIONS, "INSERT INTO").fetchSize(1);

        statement.execute().as(StepVerifier::create)
            .verifyComplete();

        ArgumentCaptor<Publisher<Message>> captor = ArgumentCaptor.forClass(Publisher.class);

        verify(client).exchange((Publisher) captor.capture(), any(Predicate.class));

        StepVerifier.create(captor.getValue())
            .consumeNextWith(it -> assertThat(it)
                .isInstanceOf(RpcRequest.class))
            .thenCancel()
            .verify();
    }

    @SuppressWarnings("unchecked")
    private static Client mockClient() {

        Client client = mock(Client.class);

        when(client.getRequiredCollation()).thenReturn(Collation.RAW);
        when(client.getTransactionDescriptor()).thenReturn(TransactionDescriptor.empty());
        when(client.exchange(any(Publisher.class), any(Predicate.class))).thenReturn(Flux.empty());
        when(client.getContext()).thenReturn(new ConnectionContext());

        return client;
    }

    private static Column createColumn(int index, String name, SqlServerType serverType, int length, LengthStrategy lengthStrategy, @Nullable Charset charset) {

        Builder builder = TypeInformation.builder().withServerType(serverType).withMaxLength(length).withLengthStrategy(lengthStrategy);
        if (charset != null) {
            builder.withCharset(charset);
        }
        TypeInformation type = builder.build();

        return new Column(index, name, type, null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select", "SELECT", "sElEcT"})
    void shouldAcceptQueries(String query) {
        assertThat(SimpleMssqlStatement.prefersCursors(query)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {" select", "sp_cursor", "INSERT"})
    void shouldRejectQueries(String query) {
        assertThat(SimpleMssqlStatement.prefersCursors(query)).isFalse();
    }
}
