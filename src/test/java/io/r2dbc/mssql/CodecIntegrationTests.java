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

import io.r2dbc.mssql.codec.DefaultCodecs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * Integration tests for {@link DefaultCodecs} testing all known codecs with pre-defined values and {@literal null} values.
 *
 * @author Mark Paluch
 */
class CodecIntegrationTests {

    private static MssqlConnectionFactory connectionFactory;

    private static MssqlConnection connection;

    @BeforeAll
    static void setUp() {

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .host("localhost")
            .username("sa")
            .password("my1.password")
            .database("foo")
            .build();

        connectionFactory = new MssqlConnectionFactory(configuration);
        connection = connectionFactory.create().block();
    }

    @AfterAll
    static void afterAll() {
        connection.close().subscribe();
    }

    @Test
    void shouldEncodeBooleanAsBit() {
        testType(connection, "BIT", true);
    }

    @Test
    void shouldEncodeBooleanAsTinyint() {
        testType(connection, "TINYINT", true, (byte) 1);
    }

    @Test
    void shouldEncodeByteAsTinyint() {
        testType(connection, "TINYINT", (byte) 0x42);
    }

    @Test
    void shouldEncodeShortAsSmallint() {
        testType(connection, "SMALLINT", Short.MAX_VALUE);
    }

    @Test
    void shouldEncodeIntegerAInt() {
        testType(connection, "INT", Integer.MAX_VALUE);
    }

    @Test
    void shouldEncodeLongABigint() {
        testType(connection, "BIGINT", Long.MAX_VALUE);
    }

    @Test
    void shouldEncodeFloatAsReal() {
        testType(connection, "REAL", Float.MAX_VALUE);
    }

    @Test
    void shouldEncodeDoubleAsFloat() {
        testType(connection, "FLOAT", Double.MAX_VALUE);
    }

    @Test
    void shouldEncodeDoubleAsNumeric() {
        testType(connection, "NUMERIC(38,5)", new BigDecimal("12345.12345"));
    }

    @Test
    void shouldEncodeDoubleAsDecimal() {
        testType(connection, "DECIMAL(38,5)", new BigDecimal("12345.12345"));
    }

    @Test
    void shouldEncodeDate() {
        testType(connection, "DATE", LocalDate.parse("2018-11-08"));
    }

    @Test
    void shouldEncodeTime() {
        testType(connection, "TIME", LocalTime.parse("11:08:27.1"));
    }

    @Test
    void shouldEncodeDateTime() {
        testType(connection, "DATETIME", LocalDateTime.parse("2018-11-08T11:08:28.2"));
    }

    @Test
    void shouldEncodeDateTime2() {
        testType(connection, "DATETIME2", LocalDateTime.parse("2018-11-08T11:08:28.2"));
    }

    @Test
    void shouldEncodeZonedDateTimeAsDatetimeoffset() {
        testType(connection, "DATETIMEOFFSET", ZonedDateTime.parse("2018-08-27T17:41:14.890+00:45[UT+00:45]"));
    }

    @Test
    void shouldEncodeGuid() {
        testType(connection, "uniqueidentifier", UUID.randomUUID());
    }

    @Test
    void shouldEncodeStringAsVarchar() {
        testType(connection, "VARCHAR(255)", "Hello, World!");
    }

    @Test
    void shouldEncodeStringAsNVarchar() {
        testType(connection, "NVARCHAR(255)", "Hello, World!");
    }

    private void testType(MssqlConnection connection, String columnType, Object value) {
        testType(connection, columnType, value, value);
    }

    private void testType(MssqlConnection connection, String columnType, Object value, Object expectedGetObjectValue) {

        createTable(connection, columnType);

        connection.createStatement("INSERT INTO codec_test values(@P0)")
            .bind("P0", value)
            .execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM codec_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> (Object) row.get("my_col", value.getClass())))
            .as(StepVerifier::create)
            .expectNext(value)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM codec_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col")))
            .as(StepVerifier::create)
            .expectNext(expectedGetObjectValue)
            .verifyComplete();

        connection.createStatement("UPDATE codec_test SET my_col = @P0")
            .bindNull("P0", value.getClass())
            .execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM codec_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable((Object) row.get("my_col", value.getClass()))))
            .as(StepVerifier::create)
            .expectNext(Optional.empty())
            .verifyComplete();
    }

    private void createTable(MssqlConnection connection, String columnType) {

        connection.createStatement("DROP TABLE codec_test").execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE codec_test (my_col " + columnType + ")")
                .execute().flatMap(MssqlResult::getRowsUpdated))
            .as(StepVerifier::create)
            .verifyComplete();
    }
}
