/*
 * Copyright 2019-2021 the original author or authors.
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
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Iterator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DefaultMssqlResult}.
 *
 * @author Mark Paluch
 */
class MssqlResultUnitTests {

    @Test
    void shouldDeferErrorSignal() {

        ErrorToken error = new ErrorToken(0, 0, Byte.MIN_VALUE, Byte.MIN_VALUE, "foo", "", "", 0);
        DoneToken done = DoneToken.create(0);
        Iterator<Message> iterator = Stream.of(error, done).map(Message.class::cast).iterator();

        MssqlResult result = MssqlSegmentResult.toResult("", new ConnectionContext(), new DefaultCodecs(), Flux.fromIterable(() -> iterator), false);

        result.getRowsUpdated()
            .as(StepVerifier::create)
            .expectError()
            .verify();

        assertThat(iterator.hasNext()).isFalse();
    }
}
