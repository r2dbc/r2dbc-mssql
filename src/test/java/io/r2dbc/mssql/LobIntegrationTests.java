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

import io.r2dbc.mssql.codec.DefaultCodecs;
import io.r2dbc.mssql.util.IntegrationTestSupport;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link DefaultCodecs} testing all known codecs with pre-defined values and {@code null} values.
 *
 * @author Mark Paluch
 */
class LobIntegrationTests extends IntegrationTestSupport {

    static byte[] ALL_BYTES = new byte[-(-128) + 127];

    static {
        for (int i = -128; i < 127; i++) {
            ALL_BYTES[-(-128) + i] = (byte) i;
        }
    }

    @Test
    void testNullBlob() {

        createTable(connection, "IMAGE");

        Flux.from(connection.createStatement("INSERT INTO lob_test values(@P0)")
                .bindNull("P0", Blob.class)
                .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable(row.get("my_col", Blob.class))))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> assertThat(actual).isEqualTo(Optional.empty()))
            .verifyComplete();
    }

    @Test
    void testSmallBlob() {

        createTable(connection, "IMAGE");

        Flux.from(connection.createStatement("INSERT INTO lob_test values(@P0)")
                .bind("P0", Blob.from(Mono.just("foo".getBytes()).map(ByteBuffer::wrap)))
                .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Blob.class)))
            .flatMap(Blob::stream)
            .as(StepVerifier::create)
            .consumeNextWith(actual -> assertThat(actual).isEqualTo(ByteBuffer.wrap("foo".getBytes())))
            .verifyComplete();
    }

    @Test
    void testBigBlob() {

        int count = 1500; // ~ 382kb
        createTable(connection, "VARBINARY(MAX)");

        Flux.from(connection.createStatement("INSERT INTO lob_test values(@P0)")
                .bind("P0", Blob.from(Flux.range(0, count).map(it -> ByteBuffer.wrap(ALL_BYTES))))
                .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Blob.class)))
            .flatMap(Blob::stream)
            .map(Buffer::remaining)
            .collect(Collectors.summingInt(value -> value))
            .as(StepVerifier::create)
            .expectNext(count * ALL_BYTES.length)
            .verifyComplete();
    }

    @Test
    void testBigBlobs() {

        int count = 512000;
        createTable(connection, "NVARCHAR(MAX)");

        CharBuffer chars = CharBuffer.allocate(count);
        IntStream.range(0, count).forEach(i -> chars.put((char) ((i % 26) + 'a')));
        chars.flip();
        String data = chars.toString();

        for (int i = 0; i < 30; i++) {
            Flux.from(connection.createStatement("INSERT INTO lob_test values(@P0)")
                    .bind("P0", data)
                    .execute())
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1L)
                .verifyComplete();

        }

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .concatMap(it -> it.map((row, rowMetadata) -> row.get("my_col")))
            .as(StepVerifier::create)
            .expectNextCount(30)
            .verifyComplete();
    }

    @Test
    void testByteArrayBlob() {

        int count = 1500; // ~ 382kb
        createTable(connection, "VARBINARY(MAX)");

        byte[] bytes = new byte[count * ALL_BYTES.length];

        for (int i = 0; i < count; i++) {
            System.arraycopy(ALL_BYTES, 0, bytes, i * ALL_BYTES.length, ALL_BYTES.length);
        }

        Flux.from(connection.createStatement("INSERT INTO lob_test values(@P0)")
                .bind("P0", bytes)
                .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Blob.class)))
            .flatMap(Blob::stream)
            .map(Buffer::remaining)
            .collect(Collectors.summingInt(value -> value))
            .as(StepVerifier::create)
            .expectNext(count * ALL_BYTES.length)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", byte[].class)))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> assertThat(actual).isEqualTo(bytes))
            .verifyComplete();
    }

    @Test
    void testNullClob() {

        createTable(connection, "NTEXT");

        Flux.from(connection.createStatement("INSERT INTO lob_test values(@P0)")
                .bindNull("P0", Clob.class)
                .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable(row.get("my_col", Clob.class))))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> assertThat(actual).isEqualTo(Optional.empty()))
            .verifyComplete();
    }

    @Test
    void testSmallClob() {

        createTable(connection, "NTEXT");

        Flux.from(connection.createStatement("INSERT INTO lob_test values(@P0)")
                .bind("P0", Clob.from(Mono.just("foo")))
                .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Clob.class)))
            .flatMap(Clob::stream)
            .as(StepVerifier::create)
            .consumeNextWith(actual -> assertThat(actual).isEqualTo("foo"))
            .verifyComplete();
    }

    private void createTable(MssqlConnection connection, String columnType) {

        connection.createStatement("DROP TABLE lob_test").execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE lob_test (my_col " + columnType + ")")
                .execute().flatMap(MssqlResult::getRowsUpdated))
            .as(StepVerifier::create)
            .verifyComplete();
    }
}
