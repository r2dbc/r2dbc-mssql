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

import io.r2dbc.mssql.client.ConnectionContext;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.NbcRowToken;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link DefaultMssqlResult}.
 *
 * @author Mark Paluch
 */
class MssqlResultUnitTests {

    @ParameterizedTest
    @MethodSource("factories")
    void shouldEmitErrorSignalInOrder(ResultFactory factory) {

        ErrorToken error = new ErrorToken(0, 0, Byte.MIN_VALUE, Byte.MIN_VALUE, "foo", "", "", 0);
        DoneToken done = DoneToken.create(0);

        MssqlResult countThenError = factory.create(Flux.just(done, error));

        countThenError.getRowsUpdated()
            .as(StepVerifier::create)
            .expectError()
            .verify();

        MssqlResult errorThenCount = factory.create(Flux.just(error, done));

        errorThenCount.getRowsUpdated()
            .as(StepVerifier::create)
            .expectError()
            .verify();
    }

    static List<ResultFactory> factories() {

        return Arrays.asList(new ResultFactory() {

            @Override
            MssqlResult create(Flux<Message> messages) {
                return DefaultMssqlResult.toResult("", new ConnectionContext(), new DefaultCodecs(), messages, false);
            }

            @Override
            public String toString() {
                return "DefaultMssqlResult";
            }
        }, new ResultFactory() {

            @Override
            MssqlResult create(Flux<Message> messages) {
                return MssqlSegmentResult.toResult("", new ConnectionContext(), new DefaultCodecs(), messages, false);
            }

            @Override
            public String toString() {
                return "MssqlSegmentResult";
            }
        });
    }

    static abstract class ResultFactory {

        abstract MssqlResult create(Flux<Message> messages);

    }

    @Test
    void mapSkipsDeletedKeysetCursorRows() {

        TypeInformation intType = Types.integer();
        TypeInformation strType = Types.varchar(255);

        // A keyset-cursor fetch carries a synthesized trailing ROWSTAT (INT) column.
        Column[] columns = Arrays.asList(
            new Column(0, "id", intType),
            new Column(1, "first_name", strType),
            new Column(2, "last_name", strType),
            new Column(3, "other", strType),
            new Column(4, "other2", strType),
            new Column(5, "other3", strType),
            new Column(6, "ROWSTAT", intType)).toArray(new Column[0]);

        // Identical row layout; only the trailing ROWSTAT int differs.
        // 0x01 = row present; 0x02 = row deleted/missing placeholder (data columns are NULL/default).
        NbcRowToken present = NbcRowToken.decode(
            HexUtils.decodeToByteBuf("D2 1C 04 01 00 00 00 01 00 61 02 00 78 61 04 01 00 00 00").skipBytes(1), columns);
        NbcRowToken deleted = NbcRowToken.decode(
            HexUtils.decodeToByteBuf("D2 1C 04 01 00 00 00 01 00 61 02 00 78 61 04 02 00 00 00").skipBytes(1), columns);

        MssqlResult result = DefaultMssqlResult.toResult("", new ConnectionContext(), new DefaultCodecs(),
            Flux.just(ColumnMetadataToken.create(columns), deleted, present), false);

        // Only the present row may surface. The ROWSTAT=2 deleted placeholder must be skipped, not
        // surfaced as a phantom row. On current code both rows surface and this fails.
        result.map((row, metadata) -> "row")
            .as(StepVerifier::create)
            .expectNext("row")
            .verifyComplete();
    }

}
