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

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.client.ConnectionContext;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.token.ColInfoToken;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.ReturnValue;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import io.r2dbc.mssql.util.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DefaultMssqlResult}.
 *
 * @author Mark Paluch
 */
class MssqlResultUnitTests {

    @Test
    void mapReleasesUnconsumedReturnValues() {

        ByteBuf data = TestByteBufAllocator.TEST.buffer();
        data.writeBytes(new byte[]{(byte) 0x4, 0x42, 0, 0, 0});
        ReturnValue returnValue = new ReturnValue(6, "@out", 0, Types.integer(), data);

        MssqlResult result = DefaultMssqlResult.toResult("", new ConnectionContext(), new DefaultCodecs(),
                Flux.just(returnValue, DoneToken.create(0)), true);

        result.map((row, metadata) -> row)
                .as(StepVerifier::create)
                .verifyComplete();

        assertThat(returnValue.refCnt()).isZero();
    }

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

    @ParameterizedTest
    @MethodSource("factories")
    void shouldRetainUserColumnNamedRowstat(ResultFactory factory) {

        // SELECT CAST(2 AS int) AS ROWSTAT: no cursor layout, the column is user data.
        Column[] columns = {new Column(0, "ROWSTAT", Types.integer())};
        RowToken row = RowToken.decode(HexUtils.decodeToByteBuf("04 02 00 00 00"), columns);

        MssqlResult result = factory.create(Flux.just(ColumnMetadataToken.create(columns), row, DoneToken.create(1)));

        Flux.from(result.map((r, metadata) -> metadata.getColumnMetadatas().size() + ":" + r.get("ROWSTAT", Integer.class)))
            .as(StepVerifier::create)
            .expectNext("1:2")
            .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource("factories")
    void shouldHideVerifiedCursorRowStatusColumn(ResultFactory factory) {

        Column[] columns = {new Column(0, "id", Types.integer()), new Column(1, "ROWSTAT", Types.integer())};
        ColumnMetadataToken metadata = ColumnMetadataToken.create(columns);
        CursorColumnLayout layout = CursorColumnLayout.from(metadata, ColInfoToken.decode(HexUtils.decodeToByteBuf("06 00 01 01 08 02 00 14")));
        RowToken row = RowToken.decode(HexUtils.decodeToByteBuf("04 2A 00 00 00 04 01 00 00 00"), columns);

        MssqlResult result = factory.create(Flux.just(metadata, layout, row, DoneToken.create(1)));

        Flux.from(result.map((r, rowMetadata) -> rowMetadata.getColumnMetadatas().size() + ":" + rowMetadata.contains("ROWSTAT") + ":" + r.get("id", Integer.class)))
            .as(StepVerifier::create)
            .expectNext("1:false:42")
            .verifyComplete();
    }

}
