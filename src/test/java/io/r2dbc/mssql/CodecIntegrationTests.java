/*
 * Copyright 2018-2022 the original author or authors.
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
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.util.IntegrationTestSupport;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Type;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link DefaultCodecs} testing all known codecs with pre-defined values and {@code null} values.
 *
 * @author Mark Paluch
 */
class CodecIntegrationTests extends IntegrationTestSupport {

    static {
        Hooks.onOperatorDebug();
    }

    @Test
    void shouldEncodeBooleanAsBit() {
        testType(connection, "BIT", true);
    }

    @Test
    void shouldEncodeBooleanAsTinyint() {
        testType(connection, "TINYINT", true, Boolean.class, (byte) 1);
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
    void shouldEncodeBigIntegerAsNumeric() {
        testType(connection, "NUMERIC(38,0)", new BigInteger("12345"), BigInteger.class, new BigDecimal("12345"));
    }

    @Test
    void shouldEncodeDoubleAsDecimal() {
        testType(connection, "DECIMAL(38,5)", new BigDecimal("12345.12345"));
    }

    @Test
    void shouldEncodeDoubleAsDecimal1() {
        testType(connection, "DECIMAL(38,0)", new BigDecimal("12345"));
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
        testType(connection, "DATETIMEOFFSET", ZonedDateTime.parse("2018-08-27T17:41:14.890+00:45"), ZonedDateTime.class, OffsetDateTime.parse("2018-08-27T17:41:14.890+00:45"));
        testType(connection, "DATETIMEOFFSET", ZonedDateTime.parse("2018-08-27T17:41:14.890-01:45"), ZonedDateTime.class, OffsetDateTime.parse("2018-08-27T17:41:14.890-01:45"));
    }

    @Test
    void shouldEncodeOffsetDateTimeAsDatetimeoffset() {
        testType(connection, "DATETIMEOFFSET", OffsetDateTime.parse("2018-08-27T17:41:14.890+00:45"));
    }

    @Test
    void shouldEncodeGuid() {
        testType(connection, "uniqueidentifier", UUID.randomUUID());
    }

    @Test
    void shouldEncodeStringAsVarchar() {
        testType(connection, "VARCHAR(255)", "Hello, World!");
        testType(connection, "VARCHAR(255)", "Hello, World!", R2dbcType.VARCHAR);
        testType(connection, "VARCHAR(255)", "Hello, World!", R2dbcType.NVARCHAR);
        testType(connection, "VARCHAR(255)", "Hello, World!", SqlServerType.VARCHAR);
        testType(connection, "VARCHAR(255)", "Hello, World!", SqlServerType.NVARCHAR);
    }

    @Test
    void shouldEncodeStringAsNVarchar() {
        testType(connection, "NVARCHAR(255)", "Hello, World! äöü");
        testType(connection, "NVARCHAR(255)", "Hello, World! äöü", R2dbcType.NVARCHAR);
        testType(connection, "NVARCHAR(255)", "Hello, World!äöü", SqlServerType.NVARCHAR);
    }

    @Test
    void shouldEncodeStringAsVarcharSendingCharsAsNatl() {

        ConnectionFactoryOptions options = builder().option(MssqlConnectionFactoryProvider.SEND_STRING_PARAMETERS_AS_UNICODE, false).build();
        MssqlConnection natlConnection = Mono.from(ConnectionFactories.get(options).create()).cast(MssqlConnection.class).block();

        testType(natlConnection, "VARCHAR(255)", "Hello, World!");

        natlConnection.close().block();
    }

    @Test
    void shouldEncodeStringAsVarcharMax() {
        testType(connection, "VARCHAR(MAX)", "Hello, World!");
    }

    @Test
    void shouldEncodeStringAsVarcharMaxWithBigString() {

        String template = UUID.randomUUID().toString();
        StringBuilder builder = new StringBuilder();
        IntStream.range(0, 1900).forEach(ignore -> builder.append(template));

        assertThat(builder).hasSize(68400);

        testType(connection, "VARCHAR(MAX)", builder.toString());
    }

    @Test
    void shouldEncodeStringAsNVarcharMax() {
        testType(connection, "NVARCHAR(MAX)", "Hello, World! äöü");
        testType(connection, "NVARCHAR(MAX)", "Hello, World! äöü", R2dbcType.NVARCHAR);
        testType(connection, "NVARCHAR(MAX)", "Hello, World! äöü", SqlServerType.NVARCHARMAX);
        testType(connection, "NVARCHAR(MAX)", "Hello, World! äöü", R2dbcType.VARCHAR);
    }

    @Test
    void shouldEncodeClobAsNVarcharMax() {
        testType(connection, "NVARCHAR(MAX)", Clob.from(Mono.just("Hello, World! äöü")), Clob.class, actual -> {
            assertThat(actual).isInstanceOf(Clob.class);
            Flux.from(((Clob) actual).stream()).as(StepVerifier::create).expectNext("Hello, World! äöü").verifyComplete();
        }, actual -> {
            assertThat(actual).isEqualTo("Hello, World! äöü");
        }, null);
    }

    @Test
    void shouldEncodeClobAsVarcharMaxAsNatl() {

        ConnectionFactoryOptions options = builder().option(MssqlConnectionFactoryProvider.SEND_STRING_PARAMETERS_AS_UNICODE, false).build();
        MssqlConnection natlConnection = Mono.from(ConnectionFactories.get(options).create()).cast(MssqlConnection.class).block();

        testType(natlConnection, "VARCHAR(MAX)", Clob.from(Mono.just("Hello, World!")), Clob.class, actual -> {
            assertThat(actual).isInstanceOf(Clob.class);
            Flux.from(((Clob) actual).stream()).as(StepVerifier::create).expectNext("Hello, World!").verifyComplete();
        }, actual -> {
            assertThat(actual).isEqualTo("Hello, World!");
        }, null);

        natlConnection.close().block();
    }

    @Test
    void shouldEncodeClobAsVarcharMaxAsUnicode() {

        testType(connection, "VARCHAR(MAX)", Clob.from(Mono.just("Hello, World!")), Clob.class, actual -> {
            assertThat(actual).isInstanceOf(Clob.class);
            Flux.from(((Clob) actual).stream()).as(StepVerifier::create).expectNext("Hello, World!").verifyComplete();
        }, actual -> {
            assertThat(actual).isEqualTo("Hello, World!");
        }, null);
    }

    @Test
    void shouldEncodeStringAsText() {
        testType(connection, "TEXT", "Hello, World!");
    }

    @Test
    void shouldEncodeStringAsNText() {
        testType(connection, "NTEXT", "Hello, World! äöü");
    }

    @Test
    void shouldEncodeByteBufferAsBinary() {
        testType(connection, "BINARY(9)", ByteBuffer.wrap("foobarbaz".getBytes()));
    }

    @Test
    void shouldEncodeByteBufferAsVarBinary() {
        testType(connection, "VARBINARY(9)", ByteBuffer.wrap("foobarbaz".getBytes()));
    }

    @Test
    void shouldEncodeByteArrayAsVarBinaryMax() {
        testType(connection, "VARBINARY(MAX)", "foobarbaz".getBytes(), byte[].class, actual -> assertThat(actual).isEqualTo(ByteBuffer.wrap("foobarbaz".getBytes())));
        testType(connection, "VARBINARY(MAX)", new byte[8000], byte[].class, actual -> assertThat(actual).isEqualTo(ByteBuffer.wrap(new byte[8000])));
        testType(connection, "VARBINARY(MAX)", new byte[8001], byte[].class, actual -> assertThat(actual).isEqualTo(ByteBuffer.wrap(new byte[8001])));
        testType(connection, "VARBINARY(MAX)", new byte[65534], byte[].class, actual -> assertThat(actual).isEqualTo(ByteBuffer.wrap(new byte[65534])));
        testType(connection, "VARBINARY(MAX)", new byte[65535], byte[].class, actual -> assertThat(actual).isEqualTo(ByteBuffer.wrap(new byte[65535])));
        testType(connection, "VARBINARY(MAX)", new byte[65536], byte[].class, actual -> assertThat(actual).isEqualTo(ByteBuffer.wrap(new byte[65536])));
    }

    @Test
    void shouldEncodeBlobAsVarBinaryMax() {
        testType(connection, "VARBINARY(MAX)", Blob.from(Mono.just(ByteBuffer.wrap("foobarbaz".getBytes()))), Blob.class, actual -> {

            assertThat(actual).isInstanceOf(Blob.class);
            Mono.from(((Blob) actual).discard()).subscribe();

        }, actual -> assertThat(actual).isEqualTo(ByteBuffer.wrap("foobarbaz".getBytes())), null);
    }

    @Test
    void shouldEncodeByteArrayAsImage() {
        testType(connection, "IMAGE", "foobarbaz".getBytes(), byte[].class, actual -> assertThat(actual).isEqualTo(ByteBuffer.wrap("foobarbaz".getBytes())));
    }

    @Test
    void shouldEncodeByteArrayAsBinary() {
        testType(connection, "BINARY(9)", "foobarbaz".getBytes(), byte[].class, ByteBuffer.wrap("foobarbaz".getBytes()));
    }

    @Test
    void shouldEncodeByteArrayAsVarBinary() {
        testType(connection, "VARBINARY(9)", "foobarbaz".getBytes(), byte[].class, ByteBuffer.wrap("foobarbaz".getBytes()));
    }

    @Test
    void shouldEncodeByteBufferAsVarBinaryMax() {
        testType(connection, "VARBINARY(MAX)", ByteBuffer.wrap("foobarbaz".getBytes()), ByteBuffer.class, actual -> {
            assertThat(actual).isInstanceOf(ByteBuffer.class).isEqualTo(ByteBuffer.wrap("foobarbaz".getBytes()));
        });
    }

    @Test
    void shouldEncodeByteBufferAsImage() {
        testType(connection, "IMAGE", ByteBuffer.wrap("foobarbaz".getBytes()), ByteBuffer.class, actual -> {
            assertThat(actual).isInstanceOf(ByteBuffer.class).isEqualTo(ByteBuffer.wrap("foobarbaz".getBytes()));
        });
    }

    private void testType(MssqlConnection connection, String columnType, Object value) {
        testType(connection, columnType, value, value.getClass(), value, null);
    }

    private void testType(MssqlConnection connection, String columnType, Object value, @Nullable Type parameterValueType) {
        testType(connection, columnType, value, value.getClass(), value, parameterValueType);
    }

    private void testType(MssqlConnection connection, String columnType, Object value, Class<?> valueClass, Object expectedGetObjectValue) {
        testType(connection, columnType, value, valueClass, actual -> assertThat(actual).isEqualTo(value), actual -> assertThat(actual).isEqualTo(expectedGetObjectValue), null);
    }

    private void testType(MssqlConnection connection, String columnType, Object value, Class<?> valueClass, Object expectedGetObjectValue, @Nullable Type parameterValueType) {
        testType(connection, columnType, value, valueClass, actual -> assertThat(actual).isEqualTo(value), actual -> assertThat(actual).isEqualTo(expectedGetObjectValue), parameterValueType);
    }

    private void testType(MssqlConnection connection, String columnType, Object value, Class<?> valueClass, Consumer<Object> nativeValueConsumer) {
        testType(connection, columnType, value, valueClass, actual -> assertThat(actual).isEqualTo(value), nativeValueConsumer, null);
    }

    private void testType(MssqlConnection connection, String columnType, Object value, Class<?> valueClass, Consumer<Object> expectedValueConsumer, Consumer<Object> nativeValueConsumer,
                          @Nullable Type parameterValueType) {

        createTable(connection, columnType);

        if (parameterValueType == null) {
            Flux.from(connection.createStatement("INSERT INTO codec_test values(@P0)")
                .bind("P0", value)
                .execute())
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();
        } else {
            Flux.from(connection.createStatement("INSERT INTO codec_test values(@P0)")
                .bind("P0", Parameters.in(parameterValueType, value))
                .execute())
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();
        }

        if (value instanceof ByteBuffer) {
            ((ByteBuffer) value).rewind();
        }

        connection.createStatement("SELECT my_col FROM codec_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> (Object) row.get("my_col", valueClass)))
            .as(StepVerifier::create)
            .consumeNextWith(expectedValueConsumer)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM codec_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col")))
            .as(StepVerifier::create)
            .consumeNextWith(nativeValueConsumer)
            .verifyComplete();

        if (parameterValueType == null) {
            Flux.from(connection.createStatement("UPDATE codec_test SET my_col = @P0")
                .bindNull("P0", value.getClass())
                .execute())
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();

            connection.createStatement("SELECT my_col FROM codec_test")
                .execute()
                .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable((Object) row.get("my_col", valueClass))))
                .as(StepVerifier::create)
                .expectNext(Optional.empty())
                .verifyComplete();

            Flux.from(connection.createStatement("UPDATE codec_test SET my_col = @P0")
                .bind("P0", Parameters.in(value.getClass()))
                .execute())
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();

            connection.createStatement("SELECT my_col FROM codec_test")
                .execute()
                .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable((Object) row.get("my_col", valueClass))))
                .as(StepVerifier::create)
                .expectNext(Optional.empty())
                .verifyComplete();
        } else {

            Flux.from(connection.createStatement("UPDATE codec_test SET my_col = @P0")
                .bind("P0", Parameters.in(parameterValueType))
                .execute())
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();

            connection.createStatement("SELECT my_col FROM codec_test")
                .execute()
                .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable((Object) row.get("my_col", valueClass))))
                .as(StepVerifier::create)
                .expectNext(Optional.empty())
                .verifyComplete();
        }
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
