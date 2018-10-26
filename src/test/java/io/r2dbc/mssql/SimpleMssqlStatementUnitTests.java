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

import io.r2dbc.mssql.client.TestClient;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.tds.Encode;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.DataToken;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.message.token.RowTokenFactory;
import io.r2dbc.mssql.message.token.SqlBatch;
import io.r2dbc.mssql.message.token.Tabular;
import io.r2dbc.mssql.message.type.Encoding;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.message.type.TypeInformation.SqlServerType;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.r2dbc.mssql.message.type.TypeInformation.Builder;
import static io.r2dbc.mssql.message.type.TypeInformation.LengthStrategy;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SimpleMssqlResult}.
 *
 * @author Mark Paluch
 */
class SimpleMssqlStatementUnitTests {

    static final List<Column> COLUMNS = Arrays.asList(createColumn(0, "employee_id", SqlServerType.TINYINT, 1, LengthStrategy.FIXEDLENTYPE, null),
        createColumn(1, "last_name", SqlServerType.NVARCHAR, 100, LengthStrategy.USHORTLENTYPE, Encoding.UNICODE.charset()),

        createColumn(1, "first_name", SqlServerType.VARCHAR, 50, LengthStrategy.USHORTLENTYPE, Encoding.CP1252.charset()),

        createColumn(1, "salary", SqlServerType.MONEY, 8, LengthStrategy.BYTELENTYPE, null));

    @Test
    void shouldReportNumberOfAffectedRows() {

        SqlBatch batch = SqlBatch.create(0, new TransactionDescriptor(new byte[8]), "SELECT * FROM foo");

        ColumnMetadataToken columns = ColumnMetadataToken.create(COLUMNS);

        Tabular tabular = Tabular.create(columns, DoneToken.create(1));

        TestClient client = TestClient.builder().expectRequest(batch).thenRespond(tabular.getTokens().toArray(new DataToken[0])).build();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, "SELECT * FROM foo");

        statement.execute()
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
    }

    @Test
    void shouldReturnColumnData() {

        SqlBatch batch = SqlBatch.create(0, new TransactionDescriptor(new byte[8]), "SELECT * FROM foo");

        ColumnMetadataToken columns = ColumnMetadataToken.create(COLUMNS);

        RowToken rowToken = RowTokenFactory.create(columns, buffer -> {

            Encode.asByte(buffer, 1);
            Encode.uString(buffer, "mark", StandardCharsets.UTF_16);
            Encode.uString(buffer, "paluch", StandardCharsets.US_ASCII);

            //money/salary
            buffer.writeBytes(HexUtils.decodeToByteBuf("080000000020A10700"));
        });

        Tabular tabular = Tabular.create(columns, rowToken, DoneToken.create(1));

        TestClient client = TestClient.builder().expectRequest(batch).thenRespond(tabular.getTokens().toArray(new DataToken[0])).build();

        SimpleMssqlStatement statement = new SimpleMssqlStatement(client, "SELECT * FROM foo");

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

    private static Column createColumn(int index, String name, SqlServerType serverType, int length, LengthStrategy lengthStrategy, @Nullable Charset charset) {

        Builder builder = TypeInformation.builder().withServerType(serverType).withMaxLength(length).withLengthStrategy(lengthStrategy);
        if (charset != null) {
            builder.withCharset(charset);
        }
        TypeInformation type = builder.build();

        return new Column(index, name, type, null);
    }
}
