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

import io.r2dbc.mssql.util.IntegrationTestSupport;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link MssqlColumnMetadata} testing all known types. This is an integration test because we want the server to produce type information.
 *
 * @author Mark Paluch
 */
class ColumnMetadataIntegrationTests extends IntegrationTestSupport {

    @Test
    void shouldReportBit() {
        testType("BIT", true, new Expectation(1, 0, Boolean.class));
    }

    @Test
    void shouldReportTinyint() {
        testType("TINYINT", 0x42, new Expectation(3, 0, Byte.class));
    }

    @Test
    void shouldReportSmallint() {
        testType("SMALLINT", Short.MAX_VALUE, new Expectation(5, 0, Short.class));
    }

    @Test
    void shouldReportInt() {
        testType("INT", Integer.MAX_VALUE, new Expectation(10, 0, Integer.class));
    }

    @Test
    void shouldReportBigint() {
        testType("BIGINT", Long.MAX_VALUE, new Expectation(19, 0, Long.class));
    }

    @Test
    void shouldReportReal() {
        testType("REAL", Float.MAX_VALUE, new Expectation(7, 0, Float.class));
    }

    @Test
    void shouldReportFloat() {
        testType("FLOAT", Float.MAX_VALUE, new Expectation(15, 0, Double.class));
        testType("FLOAT(48)", Float.MAX_VALUE, new Expectation(15, 0, Double.class));
    }

    @Test
    void shouldReportNumeric() {
        testType("NUMERIC(38,5)", new BigDecimal("12345.12345"), new Expectation(38, 5, BigDecimal.class));
    }

    @Test
    void shouldReportDecimal() {
        testType("DECIMAL(38,5)", new BigDecimal("12345.12345"), new Expectation(38, 5, BigDecimal.class));
    }

    @Test
    void shouldReportDate() {
        testType("DATE", LocalDate.parse("2018-11-08"), new Expectation(10, 0, LocalDate.class));
    }

    @Test
    void shouldReportTime() {
        testType("TIME", LocalTime.parse("11:08:27.1"), new Expectation(16, 7, LocalTime.class));
    }

    @Test
    void shouldReportDateTime() {
        testType("DATETIME", LocalDateTime.parse("2018-11-08T11:08:28.2"), new Expectation(23, 3, LocalDateTime.class));
    }

    @Test
    void shouldReportDateTime2() {
        testType("DATETIME2", LocalDateTime.parse("2018-11-08T11:08:28.2"), new Expectation(27, 7, LocalDateTime.class));
    }

    @Test
    void shouldReportDatetimeoffset() {
        testType("DATETIMEOFFSET", ZonedDateTime.parse("2018-08-27T17:41:14.890+00:45"), new Expectation(34, 7, OffsetDateTime.class));
    }

    @Test
    void shouldReportUuid() {
        testType("uniqueidentifier", UUID.randomUUID(), new Expectation(36, 0, UUID.class));
    }

    @Test
    void shouldReportVarchar() {
        testType("VARCHAR(255)", "Hello, World!", new Expectation(255, 0, String.class));
    }

    @Test
    void shouldEncodeStringAsNVarchar() {
        testType("NVARCHAR(200)", "Hello, World!", new Expectation(200, 0, String.class));
    }

    private void testType(String columnType, Object value, Expectation expectation) {

        createTable(connection, columnType);

        Flux.from(connection.createStatement("INSERT INTO metadata_test values(@P0, @P0)")
                .bind("P0", value)
                .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createStatement("SELECT non_nullable_col FROM metadata_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> rowMetadata.getColumnMetadata("non_nullable_col")))
            .as(StepVerifier::create)
            .consumeNextWith(it -> {

                assertThat(it.getNullability()).isEqualTo(Nullability.NON_NULL);
                assertThat(it.getPrecision()).isEqualTo(expectation.precision);
                assertThat(it.getScale()).isEqualTo(expectation.scale);
                assertThat(it.getJavaType()).isEqualTo(expectation.javaClass);

            })
            .verifyComplete();

        connection.createStatement("SELECT nullable_col FROM metadata_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> rowMetadata.getColumnMetadata("nullable_col")))
            .as(StepVerifier::create)
            .consumeNextWith(it -> {

                assertThat(it.getNullability()).isEqualTo(Nullability.NULLABLE);
                assertThat(it.getPrecision()).isEqualTo(expectation.precision);
                assertThat(it.getScale()).isEqualTo(expectation.scale);
                assertThat(it.getJavaType()).isEqualTo(expectation.javaClass);

            })
            .verifyComplete();
    }

    private void createTable(MssqlConnection connection, String columnType) {

        connection.createStatement("DROP TABLE metadata_test").execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE metadata_test (nullable_col " + columnType + " NULL, non_nullable_col " + columnType + "  NOT NULL)")
                .execute().flatMap(MssqlResult::getRowsUpdated))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    /**
     * Expectations wrapper.
     */
    static class Expectation {

        final int precision;

        final int scale;

        final Class<?> javaClass;

        Expectation(int precision, int scale, Class<?> javaClass) {
            this.precision = precision;
            this.scale = scale;
            this.javaClass = javaClass;
        }
    }
}
