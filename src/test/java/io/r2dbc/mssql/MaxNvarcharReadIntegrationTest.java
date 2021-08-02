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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.r2dbc.mssql.util.IntegrationTestSupport;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Integration test for checking that the message backpressure
 * is handled correctly in case the result payload is very large.
 *
 * @author azukovskij
 * @author gvisokinskas
 */
public class MaxNvarcharReadIntegrationTest extends IntegrationTestSupport {
    private static final long TIMEOUT_MS = 30000L;
    private static final int ROW_COUNT = Integer.getInteger("r2dbc.mssql.test.row.count", 30);
    private static final int DATA_LENGTH = Integer.getInteger("r2dc.mssql.test.data.length", 512000);

    @Test
    public void shouldReturnLargeSelectResult() {
        populateData().block();

        List<?> results = Mono.from(connectionFactory.create())
            .flatMapMany(this::query)
            .collectList()
            .timeout(Duration.ofSeconds(TIMEOUT_MS))
            .block();

        assertThat(results.size()).isEqualTo(ROW_COUNT);
    }

    private Flux<Object> query(Connection c) {
        return Flux.from(c.createStatement("select * from test.Test").execute())
            .flatMap(r -> r.map((row, m) -> row.get(0)));
    }

    private static Flux<Integer> execute(Connection c, String sql) {
        return Flux.defer(() -> Mono.from(c.createStatement(sql).execute()).flatMapMany(Result::getRowsUpdated));
    }

    private static Mono<Integer> populateData() {
        return Mono.from(connectionFactory.create())
            .flatMapMany(c -> execute(c, "CREATE SCHEMA test")
                .concatWith(execute(c, "CREATE TABLE test.Test (id int PRIMARY KEY, data nvarchar(max))"))
                .concatWith(Flux.range(0, ROW_COUNT)
                    .flatMap(i -> {
                        String data = IntStream
                            .range(0, DATA_LENGTH).mapToObj(String::valueOf).collect(Collectors.joining());
                        return execute(c, "INSERT INTO test.Test(id, data) VALUES(" + i + ", '" + data + "');");
                    })))
            .ignoreElements();
    }
}
