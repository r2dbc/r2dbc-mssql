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
import io.r2dbc.mssql.message.token.DoneToken;
import io.r2dbc.mssql.message.token.ErrorToken;
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

}
