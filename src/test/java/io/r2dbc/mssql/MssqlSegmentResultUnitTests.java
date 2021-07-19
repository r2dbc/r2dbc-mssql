/*
 * Copyright 2021 the original author or authors.
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
import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.client.ConnectionContext;
import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.message.token.ColumnMetadataToken;
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ErrorToken;
import io.r2dbc.mssql.message.token.InfoToken;
import io.r2dbc.mssql.message.token.NbcRowToken;
import io.r2dbc.mssql.message.token.ReturnValue;
import io.r2dbc.mssql.message.token.RowToken;
import io.r2dbc.mssql.message.type.TypeInformation;
import io.r2dbc.mssql.util.HexUtils;
import io.r2dbc.mssql.util.Types;
import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MssqlSegmentResult}.
 *
 * @author Mark Paluch
 */
class MssqlSegmentResultUnitTests {

    ErrorToken errorToken = new ErrorToken(0, 0, 0, 0, "error message desc", "", "", 0);

    InfoToken infoToken = new InfoToken(0, 0, 0, 0, "error message desc", "", "", 0);

    TypeInformation integerType = Types.integer();

    TypeInformation stringType = Types.varchar(255);

    Column[] columns = Arrays.asList(new Column(0, "id", integerType),
        new Column(1, "first_name", stringType),
        new Column(2, "last_name", stringType),
        new Column(3, "other", stringType),
        new Column(4, "other2", stringType),
        new Column(5, "other3", stringType),
        new Column(6, "rowstat", integerType)).toArray(new Column[0]);

    DefaultCodecs codecs = new DefaultCodecs();

    private NbcRowToken getRowToken() {
        return NbcRowToken.decode(HexUtils.decodeToByteBuf("D2 1C 04 01 00 00 00 01 00 61 02 00 78 61 04 01 00 00 00").skipBytes(1), columns);
    }

    @Test
    void shouldApplyRowMapping() {

        MssqlSegmentResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(ColumnMetadataToken.create(columns), getRowToken()), false);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void shouldApplyOutParameterMapping() {

        ByteBuf buffer = HexUtils.decodeToByteBuf("AC0000000100000000000026" +
            "0404F3DEBC0A").skipBytes(1);

        MssqlSegmentResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(ReturnValue.decode(buffer, false)), true);

        result.map((readable) -> readable)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void mapShouldIgnoreNotice() {

        MssqlSegmentResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(infoToken), false);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void mapShouldTerminateWithError() {

        MssqlSegmentResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(errorToken), false);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientException.class);
    }

    @Test
    void getRowsUpdatedShouldTerminateWithError() {

        MssqlSegmentResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(errorToken), false);

        result.getRowsUpdated()
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientException.class);
    }

    @Test
    void shouldConsumeRowsUpdated() {

        MssqlSegmentResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(DoneToken.count(42)), false);

        result.getRowsUpdated()
            .as(StepVerifier::create)
            .expectNext(42)
            .verifyComplete();
    }

    @Test
    void filterShouldRetainUpdateCount() {

        MssqlSegmentResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(DoneToken.count(42)), false);

        result.filter(Result.UpdateCount.class::isInstance).getRowsUpdated()
            .as(StepVerifier::create)
            .expectNext(42)
            .verifyComplete();
    }

    @Test
    void filterShouldSkipRowMapping() {

        MssqlResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(ColumnMetadataToken.create(columns), getRowToken()), false);

        result = result.filter(it -> false);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void filterShouldSkipErrorMessage() {

        MssqlResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(errorToken, ColumnMetadataToken.create(columns), getRowToken()), false);

        result = result.filter(Result.RowSegment.class::isInstance);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void mapRowShouldDeallocateRowResources(boolean expectReturnValues) {

        ByteBuf buffer = HexUtils.decodeToByteBuf("AC0000000100000000000026" +
            "0404F3DEBC0A").skipBytes(1);

        ReturnValue returnValue = ReturnValue.decode(buffer, false);
        buffer.release();

        RowToken dataRow = getRowToken();
        assertThat(dataRow.refCnt()).isOne();
        MssqlSegmentResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(ColumnMetadataToken.create(columns), dataRow, returnValue), expectReturnValues);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        assertThat(dataRow.refCnt()).isZero();
        assertThat(returnValue.refCnt()).isZero();
        assertThat(buffer.refCnt()).isZero();
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void mapShouldDeallocateRowResources(boolean expectReturnValues) {

        ByteBuf buffer = HexUtils.decodeToByteBuf("AC0000000100000000000026" +
            "0404F3DEBC0A").skipBytes(1);

        ReturnValue returnValue = ReturnValue.decode(buffer, false);
        buffer.release();

        RowToken dataRow = getRowToken();
        assertThat(dataRow.refCnt()).isOne();
        MssqlSegmentResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(ColumnMetadataToken.create(columns), dataRow, returnValue), expectReturnValues);

        result.map(Function.identity())
            .as(StepVerifier::create)
            .expectNextCount(expectReturnValues ? 2 : 1)
            .verifyComplete();

        assertThat(dataRow.refCnt()).isZero();
        assertThat(returnValue.refCnt()).isZero();
        assertThat(buffer.refCnt()).isZero();
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void filterShouldDeallocateResources(boolean expectReturnValues) {

        ByteBuf buffer = HexUtils.decodeToByteBuf("AC0000000100000000000026" +
            "0404F3DEBC0A").skipBytes(1);

        ReturnValue returnValue = ReturnValue.decode(buffer, false);
        buffer.release();

        RowToken dataRow = getRowToken();
        assertThat(dataRow.refCnt()).isOne();
        MssqlResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(ColumnMetadataToken.create(columns), dataRow, returnValue), expectReturnValues);

        result = result.filter(it -> false);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(dataRow.refCnt()).isZero();
        assertThat(returnValue.refCnt()).isZero();
        assertThat(buffer.refCnt()).isZero();
    }

    @Test
    void flatMapShouldDeallocateResourcesAfterConsumption() {

        RowToken dataRow = getRowToken();
        assertThat(dataRow.refCnt()).isOne();
        MssqlResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(ColumnMetadataToken.create(columns), dataRow), false);

        Flux.from(result.flatMap(Mono::just))
            .map(it -> {
                assertThat(((ReferenceCounted) it).refCnt()).isOne();
                return it;
            })
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        assertThat(dataRow.refCnt()).isZero();
    }

    @Test
    void flatMapShouldNotTerminateWithError() {

        MssqlResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(errorToken, ColumnMetadataToken.create(columns), getRowToken(), DoneToken.create(42)), false);

        Flux.from(result.flatMap(Mono::just))
            .as(StepVerifier::create)
            .expectNextCount(3)
            .verifyComplete();
    }

    @Test
    void emptyFlatMapShouldDeallocateResourcesAfterConsumption() {

        RowToken dataRow = getRowToken();
        assertThat(dataRow.refCnt()).isOne();
        MssqlResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(ColumnMetadataToken.create(columns), dataRow), false);

        Flux.from(result.flatMap(data -> Mono.empty()))
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(dataRow.refCnt()).isZero();
    }

    @Test
    void flatMapShouldMapErrorResponse() {

        MssqlResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(errorToken), false);

        Flux.from(result.flatMap(data -> {

            assertThat(data).isInstanceOf(Result.Message.class);

            Result.Message message = (Result.Message) data;
            assertThat(message.errorCode()).isZero();
            assertThat(message.sqlState()).isEqualTo("S0000");
            assertThat(message.message()).isEqualTo("error message desc");
            assertThat(message.exception()).isInstanceOf(R2dbcNonTransientResourceException.class);

            return Mono.just(data);
        }))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void flatMapShouldMapNoticeResponse() {

        MssqlResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), codecs, Flux.just(infoToken), false);

        Flux.from(result.flatMap(data -> {

            assertThat(data).isInstanceOf(Result.Message.class);

            Result.Message message = (Result.Message) data;
            assertThat(message.errorCode()).isZero();
            assertThat(message.sqlState()).isEqualTo("S0000");
            assertThat(message.message()).isEqualTo("error message desc");
            assertThat(message.exception()).isInstanceOf(R2dbcNonTransientResourceException.class);

            return Mono.just(data);
        }))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

}
